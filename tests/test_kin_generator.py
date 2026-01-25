import json
import os
import pytest
from kinGenerator import generate_kin_report_from_files, KinReportGenerator

@pytest.mark.parametrize("test_dir_name", ["test1", "test2"])
def test_generate_kin_report_logic_and_format(tmp_path, monkeypatch, test_dir_name):
    # Setup paths
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), f"data/kinGenerator/{test_dir_name}"))

    # Find all json files in the directory
    all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".json")]

    # Input files are those that don't have "kin_report" in their name
    input_paths = [f for f in all_files if "kin_report" not in os.path.basename(f)]

    # Reference report is the one that has "kin_report" in its name
    reference_reports = [f for f in all_files if "kin_report" in os.path.basename(f)]
    assert len(reference_reports) == 1, f"Expected exactly one reference report in {data_dir}, found {len(reference_reports)}"
    reference_report_path = reference_reports[0]

    with open(reference_report_path, 'r', encoding='utf-8') as f:
        reference_data = json.load(f)

    # Load all available source codes for verification
    source_codes = set()
    for p in input_paths:
        with open(p, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
            if 'codes' in data:
                source_codes.update(data['codes'])

    # Number of kits to generate should match reference
    num_kits = len(reference_data['readyBox'])

    # Run generator in tmp_path to avoid polluting project root
    monkeypatch.chdir(tmp_path)

    # The generator expects file paths. We can provide absolute paths.
    output_file = generate_kin_report_from_files(input_paths, num_kits=num_kits)

    assert output_file is not None
    assert os.path.exists(output_file)

    # Load generated data
    with open(output_file, 'r', encoding='utf-8') as f:
        generated_data = json.load(f)

    # 1. Check structure (keys)
    assert set(generated_data.keys()) == set(reference_data.keys())

    # 2. Check readyBox structure and data
    assert len(generated_data['readyBox']) == len(reference_data['readyBox'])

    all_gen_box_numbers = []
    all_gen_product_numbers = []

    generator = KinReportGenerator()

    for i, gen_box in enumerate(generated_data['readyBox']):
        # Check keys in each box
        assert "Number" in gen_box
        assert "boxNumber" in gen_box
        assert "productNumbers" in gen_box
        assert "productNumbersFull" in gen_box

        # Basic fields
        assert gen_box['Number'] == i
        assert gen_box['boxAgregate'] is True

        # Verify codes exist in source files
        assert gen_box['boxNumber'] in source_codes
        for p_full in gen_box['productNumbersFull']:
            assert p_full in source_codes

        # Verify internal consistency
        assert len(gen_box['productNumbers']) == len(gen_box['productNumbersFull'])
        for short, full in zip(gen_box['productNumbers'], gen_box['productNumbersFull']):
            assert short == generator.extract_short_code(full)

        all_gen_box_numbers.append(gen_box['boxNumber'])
        all_gen_product_numbers.extend(gen_box['productNumbersFull'])

        # Ensure boxTime is present and is a string (ISO format)
        assert isinstance(gen_box['boxTime'], str)

    # Verify uniqueness of used codes
    assert len(all_gen_box_numbers) == len(set(all_gen_box_numbers))
    assert len(all_gen_product_numbers) == len(set(all_gen_product_numbers))
    # Intersection between boxes and products should be empty (usually)
    assert set(all_gen_box_numbers).isdisjoint(set(all_gen_product_numbers))

    # Verify total counts match reference
    ref_total_products = sum(len(box['productNumbersFull']) for box in reference_data['readyBox'])
    assert len(all_gen_product_numbers) == ref_total_products

    # 3. Check other fields
    assert generated_data['operators'] == reference_data['operators']
    assert generated_data['sampleNumbers'] == reference_data['sampleNumbers']

    # 4. Check dynamic fields are present
    assert 'id' in generated_data
    assert 'startTime' in generated_data
    assert 'endTime' in generated_data
