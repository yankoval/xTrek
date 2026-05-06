import hashlib
import xml.etree.ElementTree as ET
import os
import json
import re
import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Setup logging
logger = logging.getLogger("amica_generator")
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler("amica_generator.log", maxBytes=1*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def calculate_md5(file_path):
    """Calculates MD5 hash of a file for the DataSource section in VDF."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest().upper()

def count_csv_rows(file_path):
    """Counts number of data records in the CSV/data file (excluding header)."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        # Count all non-empty lines
        total_lines = sum(1 for line in f if line.strip())

    # Subtract 1 for the header
    record_count = total_lines - 1

    if record_count <= 0:
        raise ValueError(f"Data file {file_path} contains no records (only header or empty).")

    return record_count

def string_to_hex(text):
    """Encodes string to Hex format (UTF-8) for Amica."""
    return text.encode('utf-8').hex().upper()

def hex_to_string(hex_str):
    """Decodes Hex string back to text."""
    if not hex_str:
        return ""
    try:
        return bytes.fromhex(hex_str).decode('utf-8')
    except (ValueError, TypeError):
        return ""

def find_in_json(data, target_key):
    """Recursively search for a value by key in a dictionary of any nesting."""
    if isinstance(data, dict):
        if target_key in data:
            return data[target_key]
        for v in data.values():
            res = find_in_json(v, target_key)
            if res is not None:
                return res
    elif isinstance(data, list):
        for item in data:
            res = find_in_json(item, target_key)
            if res is not None:
                return res
    return None

def apply_transformations(value, transformations):
    """Applies a list of transformations to a value."""
    current_value = value
    for trans in transformations:
        trans_type = trans.get("type")
        try:
            if trans_type == "strptime":
                fmt = trans.get("format")
                if not fmt:
                    raise ValueError("Missing 'format' for strptime transformation")
                current_value = datetime.strptime(current_value, fmt)
            elif trans_type == "strftime":
                fmt = trans.get("format")
                if not fmt:
                    raise ValueError("Missing 'format' for strftime transformation")
                if not isinstance(current_value, datetime):
                    raise TypeError(f"strftime expected datetime object, got {type(current_value)}")
                current_value = current_value.strftime(fmt)
            elif trans_type == "regex":
                pattern = trans.get("pattern")
                replacement = trans.get("replacement")
                if pattern is None or replacement is None:
                    raise ValueError("Missing 'pattern' or 'replacement' for regex transformation")
                current_value = re.sub(pattern, replacement, str(current_value))
            elif trans_type == "zfill":
                width = trans.get("width")
                if width is None:
                    raise ValueError("Missing 'width' for zfill transformation")
                current_value = str(current_value).zfill(int(width))
            else:
                raise ValueError(f"Unknown transformation type: {trans_type}")
        except Exception as e:
            logger.error(f"Transformation failed: {trans}. Value: {current_value}. Error: {e}")
            raise
    return current_value

def generate_amica_vdf(base_template_path, new_csv_path, static_json_path, mapping_json_path, output_vdf_path, filename_mask="{OriginalFileName}"):
    """Main function to generate VDF by substituting static and dynamic data."""

    # 1. Calculate MD5 and row count of the new CSV file
    new_md5 = calculate_md5(new_csv_path)
    record_count = count_csv_rows(new_csv_path)

    # 2. Load data
    with open(static_json_path, 'r', encoding='utf-8') as f:
        static_data = json.load(f)

    with open(mapping_json_path, 'r', encoding='utf-8') as f:
        mapping_list = json.load(f)

    if not isinstance(mapping_list, list):
        error_msg = "Mapping file must contain a list of dictionaries"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # 2.1 Pre-calculate transformed values and validate placeholders
    placeholder_to_value = {}
    placeholders_seen = set()

    for mapping_item in mapping_list:
        if not isinstance(mapping_item, dict):
            error_msg = f"Mapping item must be a dictionary, got {type(mapping_item)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Identify the placeholder and value source
        keys = list(mapping_item.keys())
        if len(keys) != 1:
            # Maybe it's an explicit Variant A: { "setValue": "...", "placeholder": "..." }
            if "setValue" in mapping_item and "placeholder" in mapping_item:
                placeholder = mapping_item["placeholder"]
                set_value = mapping_item["setValue"]
                transformations = mapping_item.get("transform", [])
                json_key = None
            else:
                error_msg = f"Mapping item must contain exactly one JSON key, but found {len(keys)}: {keys}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        else:
            json_key = keys[0]
            mapping_info = mapping_item[json_key]

            if isinstance(mapping_info, dict):
                if "setValue" in mapping_info:
                    # Variant B with setValue
                    placeholder = mapping_info.get("placeholder", json_key)
                    set_value = mapping_info["setValue"]
                    transformations = mapping_info.get("transform", [])
                else:
                    # Variant B with transform/placeholder
                    placeholder = mapping_info.get("placeholder")
                    if placeholder is None:
                        error_msg = f"Mapping for key '{json_key}' is missing required 'placeholder' field"
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    transformations = mapping_info.get("transform", [])
                    set_value = None
            else:
                # Simple variant
                placeholder = mapping_info
                transformations = []
                set_value = None

        # Check for duplicate placeholders
        if placeholder in placeholders_seen:
            error_msg = f"Duplicate placeholder '{placeholder}' found in mapping"
            logger.error(error_msg)
            raise ValueError(error_msg)
        placeholders_seen.add(placeholder)

        # Resolve value
        if set_value is not None:
            val = set_value
        else:
            val = find_in_json(static_data, json_key)
            if val is None:
                error_msg = f"Key '{json_key}' not found in static JSON data"
                logger.error(error_msg)
                raise KeyError(error_msg)

        if transformations:
            val = apply_transformations(val, transformations)

        placeholder_to_value[placeholder] = str(val)

    # 2.2 Resolve output filename
    original_filename = os.path.basename(output_vdf_path)
    resolved_filename = filename_mask.replace("{OriginalFileName}", original_filename)

    # Sort placeholders by length descending to prevent shadowing during replacement
    sorted_placeholders = sorted(placeholder_to_value.items(), key=lambda x: len(x[0]), reverse=True)

    for placeholder, val in sorted_placeholders:
        resolved_filename = resolved_filename.replace(f"{{{placeholder}}}", val)

    output_dir = os.path.dirname(output_vdf_path)
    final_output_path = os.path.join(output_dir, resolved_filename)

    # 3. Parse VDF template (XML)
    tree = ET.parse(base_template_path)
    root = tree.getroot()

    # 4. Update dynamic part (CSV path and MD5)
    for data_source in root.findall(".//DataSource"):
        source_path_node = data_source.find(".//SourcePath")
        if source_path_node is not None:
            # We keep the path provided in arguments, but Amica might expect Windows paths.
            # However, for testing purpose and general use, we use the provided path.
            source_path_node.text = new_csv_path

        md5_node = data_source.find(".//DataMd5")
        if md5_node is not None:
            md5_node.text = new_md5

    # 4.1 Update RipParam (Print parameters)
    rip_param = root.find(".//RipParam")
    if rip_param is not None:
        # Set total record count
        end_no = rip_param.find("EndNo")
        if end_no is not None:
            end_no.text = str(record_count)

        # Set range (e.g., "0-99" for 100 records)
        out_records = rip_param.find("OutputRecords")
        if out_records is not None:
            out_records.text = f"0-{max(0, record_count - 1)}"

    # 5. Update static part (Text blocks)
    parent_map = {c: p for p in root.iter() for c in p}
    for content_node in root.findall(".//Content"):
        if content_node.text:
            # Logic: for VariableText, we take source from TextTemplate, not Content itself.
            source_node = content_node
            is_variable_text = False
            text_template_node = None

            # Content -> Text -> VariableText
            text_node = parent_map.get(content_node)
            if text_node is not None and text_node.tag == "Text":
                var_text_node = parent_map.get(text_node)
                if var_text_node is not None and var_text_node.tag == "VariableText" and \
                   var_text_node.get("FullName") == "Amica.Vdp.Common.Element.VdpVariableText":
                    is_variable_text = True
                    text_template_node = var_text_node.find("TextTemplate")
                    if text_template_node is not None and text_template_node.text:
                        source_node = text_template_node

            decoded_text = hex_to_string(source_node.text)
            if not decoded_text:
                continue

            modified = False

            # First, check for exact match of the entire decoded text with a placeholder
            exact_match_found = False
            for placeholder, val in placeholder_to_value.items():
                if decoded_text == placeholder:
                    decoded_text = val
                    modified = True
                    exact_match_found = True
                    break

            if not exact_match_found:
                # If no exact match, replace braced {placeholder} patterns
                # using sorted placeholders to prevent shadowing (e.g., {P_long} vs {P})
                for placeholder, val in sorted_placeholders:
                    braced_placeholder = f"{{{placeholder}}}"
                    if braced_placeholder in decoded_text:
                        decoded_text = decoded_text.replace(braced_placeholder, val)
                        modified = True

            if modified:
                new_hex = string_to_hex(decoded_text)
                content_node.text = new_hex
                if is_variable_text and text_template_node is not None:
                    text_template_node.text = new_hex

    # 6. Save the result
    # short_empty_elements=False ensures <Content></Content> instead of <Content />
    # This is important for the subsequent regex replacement of Content tags.
    with open(final_output_path, 'wb') as f:
        tree.write(f, encoding="utf-8", xml_declaration=True, short_empty_elements=False)

    # 7. Final touch: wrap Hex text (or empty) in CDATA
    with open(final_output_path, "r", encoding="utf-8") as f:
        xml_str = f.read()

    # Replace content of <Content>...</Content> with CDATA
    xml_str = re.sub(r'<Content[^>]*>(.*?)</Content>', r'<Content><![CDATA[\1]]></Content>', xml_str)

    with open(final_output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)

    logger.info(f"---")
    logger.info(f"[*] File successfully created: {os.path.basename(final_output_path)}")
    logger.info(f"[*] Used CSV: {os.path.basename(new_csv_path)}")
    logger.info(f"[*] MD5: {new_md5}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Amica VDF Generator")
    parser.add_argument("--template", required=True, help="Path to base VDF template")
    parser.add_argument("--csv", required=True, help="Path to new CSV/data file")
    parser.add_argument("--json", required=True, help="Path to static JSON data")
    parser.add_argument("--mapping", default="mapping.json", help="Path to mapping JSON file")
    parser.add_argument("--output", required=True, help="Path for the output VDF file")
    parser.add_argument("--filename-mask", default="{OriginalFileName}", help="Mask for output filename (e.g. '{Product_gtin}_{OriginalFileName}')")

    args = parser.parse_args()

    try:
        generate_amica_vdf(
            base_template_path=args.template,
            new_csv_path=args.csv,
            static_json_path=args.json,
            mapping_json_path=args.mapping,
            output_vdf_path=args.output,
            filename_mask=args.filename_mask
        )
    except Exception as e:
        logger.exception(f"[!] Error: {e}")
        print(f"[!] Error: {e}")
        exit(1)
