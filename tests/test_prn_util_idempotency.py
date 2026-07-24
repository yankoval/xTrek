import json
from pathlib import Path
from unittest.mock import patch

from xtrek.prn_util import generate_prn_files


class FakeStorage:
    def __init__(self, tags=None, acquire_result=True):
        self.tags = tags or {}
        self.acquire_result = acquire_result
        self.lock_paths = []
        self.released_locks = []
        self.uploads = []

    def get_tags(self, path):
        return dict(self.tags)

    def set_tags(self, path, tags):
        self.tags.update(tags)
        return path

    def acquire_lock(self, path, content=''):
        self.lock_paths.append((path, content))
        return self.acquire_result

    def release_lock(self, path):
        self.released_locks.append(path)
        return path

    def exists(self, path):
        return True

    def download(self, remote_path, local_path):
        local = Path(local_path)
        local.parent.mkdir(parents=True, exist_ok=True)
        if str(remote_path).endswith('.json') and local.name != 'amica.json':
            local.write_text(json.dumps({"codes": ["010123456789012321ABC\u001d93XYZ"]}), encoding='utf-8')
        else:
            local.write_text("template", encoding='utf-8')
        return str(local)

    def upload(self, local_path, remote_path):
        self.uploads.append((local_path, remote_path))


def fake_config():
    return {
        "kodes": "s3://internal/kodes",
        "prn_tasks": "s3://print/solmarkTasks",
        "prn_templates": "s3://print/templates",
        "s3_config": {},
    }


def run_with_storages(kodes_storage, tasks_storage, templates_storage):
    def storage_for(path, s3_config):
        if path == "s3://internal/kodes":
            return kodes_storage
        if path == "s3://print/solmarkTasks":
            return tasks_storage
        if path == "s3://print/templates":
            return templates_storage
        raise AssertionError(f"unexpected storage path: {path}")

    def write_vdf(**kwargs):
        Path(kwargs["output_vdf_path"]).write_text("vdf", encoding="utf-8")

    with patch("xtrek.prn_util.load_config", return_value=fake_config()), \
         patch("xtrek.prn_util.get_storage", side_effect=storage_for), \
         patch("xtrek.prn_util._find_production_order_id_by_suz_order_id", return_value=None), \
         patch("xtrek.prn_util.generate_amica_vdf", side_effect=write_vdf):
        return generate_prn_files("ORDER-1")


def test_generate_prn_files_claims_lock_before_upload_and_releases_on_success():
    kodes_storage = FakeStorage(tags={"print-status": "not-printed"})
    tasks_storage = FakeStorage()
    templates_storage = FakeStorage()

    result = run_with_storages(kodes_storage, tasks_storage, templates_storage)

    assert result == "ORDER-1"
    assert tasks_storage.lock_paths[0][0] == "s3://print/solmarkTasks/.locks/ORDER-1.lock"
    assert tasks_storage.released_locks == ["s3://print/solmarkTasks/.locks/ORDER-1.lock"]
    assert kodes_storage.tags["print-status"] == "printed"
    assert [remote for _, remote in tasks_storage.uploads] == [
        "s3://print/solmarkTasks/ORDER-1.csv",
        "s3://print/solmarkTasks/ORDER-1.vdf",
    ]


def test_generate_prn_files_skips_when_another_worker_holds_lock():
    kodes_storage = FakeStorage(tags={"print-status": "not-printed"})
    tasks_storage = FakeStorage(acquire_result=False)
    templates_storage = FakeStorage()

    result = run_with_storages(kodes_storage, tasks_storage, templates_storage)

    assert result is None
    assert tasks_storage.uploads == []
    assert tasks_storage.released_locks == []
    assert kodes_storage.tags["print-status"] == "not-printed"


def test_generate_prn_files_releases_lock_when_print_was_already_done():
    kodes_storage = FakeStorage(tags={"print-status": "printed"})
    tasks_storage = FakeStorage()
    templates_storage = FakeStorage()

    result = run_with_storages(kodes_storage, tasks_storage, templates_storage)

    assert result == "ORDER-1"
    assert tasks_storage.uploads == []
    assert tasks_storage.released_locks == ["s3://print/solmarkTasks/.locks/ORDER-1.lock"]
    assert kodes_storage.tags["print-status"] == "printed"
