import json
import os
import pytest
from kinGenerator import generate_kin_report_from_files

def test_generate_kin_report_logic_and_format(tmp_path, monkeypatch):
    # Setup paths
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data/kinGenerator/test1"))
    input_files = [
        os.path.join(data_dir, f) for f in os.listdir(data_dir)
        if f.endswith(".json") and "kin_report" not in f
    ]
    reference_report_path = os.path.join(data_dir, "04640286999931_kin_report.json")

    # Run generator in tmp_path to avoid polluting project root
    monkeypatch.chdir(tmp_path)

    # The generator expects file paths. We can provide absolute paths.
    output_file = generate_kin_report_from_files(input_files)

    assert output_file is not None
    assert os.path.exists(output_file)

    # Load generated and reference data
    with open(output_file, 'r', encoding='utf-8') as f:
        generated_data = json.load(f)

    with open(reference_report_path, 'r', encoding='utf-8') as f:
        reference_data = json.load(f)

    # 1. Check structure (keys)
    assert set(generated_data.keys()) == set(reference_data.keys())

    # 2. Check readyBox structure and data
    assert len(generated_data['readyBox']) == len(reference_data['readyBox'])

    for gen_box, ref_box in zip(generated_data['readyBox'], reference_data['readyBox']):
        # Check keys in each box
        assert set(gen_box.keys()) == set(ref_box.keys())

        # Check codes (ignoring boxTime)
        assert gen_box['Number'] == ref_box['Number']
        assert gen_box['boxNumber'] == ref_box['boxNumber']
        assert gen_box['boxAgregate'] == ref_box['boxAgregate']
        assert gen_box['productNumbers'] == ref_box['productNumbers']
        assert gen_box['productNumbersFull'] == ref_box['productNumbersFull']

        # Ensure boxTime is present and is a string (ISO format)
        assert isinstance(gen_box['boxTime'], str)

    # 3. Check other fields that should be same (sampleNumbers, operators, etc.)
    assert generated_data['operators'] == reference_data['operators']
    assert generated_data['sampleNumbers'] == reference_data['sampleNumbers']
    assert generated_data['sampleNumbersFull'] == reference_data['sampleNumbersFull']
    assert generated_data['defectiveCodes'] == reference_data['defectiveCodes']
    assert generated_data['defectiveCodesFull'] == reference_data['defectiveCodesFull']
    assert generated_data['emptyNumbers'] == reference_data['emptyNumbers']

    # 4. Check dynamic fields are present
    assert 'id' in generated_data
    assert 'startTime' in generated_data
    assert 'endTime' in generated_data

    # Verify they are non-empty strings
    assert isinstance(generated_data['id'], str) and generated_data['id']
    assert isinstance(generated_data['startTime'], str) and generated_data['startTime']
    assert isinstance(generated_data['endTime'], str) and generated_data['endTime']
