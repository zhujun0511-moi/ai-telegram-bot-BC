# -*- coding: utf-8 -*-
"""
courier/dataset.py — BC 的 HF dataset 讀寫（2026-10-16「唯一的門」）

來源（逐字搬來，行為不變；HF_TOKEN 在門內讀，不再由呼叫端傳入）：
  raw_writer.HF → RawHF（list_files／read／add／flush；flush 正式寫入＝create_commit，試跑＝寫本機）
  raw_writer 收尾的 data_map.update_data_map → update_data_map
  dataset_health_check：_dl_json、_last_csv_row、repo_info 列檔
  swing_lab/bootstrap：下載程式包（latest.json → {sha}.zip）
data_map.py（vendored 純核心）也搬進 courier/，內容不變。Python 3.9 相容。
"""
import json
import os


def _token():
    return os.environ.get("HF_TOKEN", "")


# ══ raw_writer 用 ══════════════════════════════════════════════════

class RawHF:
    def __init__(self, write, repo, written_by):
        from huggingface_hub import HfApi
        self.token, self.write = _token(), write
        self.repo, self.written_by = repo, written_by
        self.api = HfApi(token=self.token)
        self.pending = []

    def list_files(self):
        return set(self.api.list_repo_files(repo_id=self.repo, repo_type="dataset"))

    def read(self, path):
        from huggingface_hub import hf_hub_download
        try:
            p = hf_hub_download(repo_id=self.repo, filename=path, repo_type="dataset", token=self.token)
        except Exception as e:
            if type(e).__name__ in ("EntryNotFoundError",) or "404" in str(e):
                return None
            raise
        with open(p, "r", encoding="utf-8") as f:
            return f.read()

    def add(self, path, text):
        self.pending.append((path, text))

    def flush(self, out_dir, note):
        if not self.pending:
            return 0
        n = len(self.pending)
        if self.write:
            from huggingface_hub import CommitOperationAdd
            ops = [CommitOperationAdd(path_in_repo=p, path_or_fileobj=t.encode("utf-8")) for p, t in self.pending]
            self.api.create_commit(repo_id=self.repo, repo_type="dataset", operations=ops,
                                   commit_message=f"{self.written_by}: {note}")
        else:
            for p, t in self.pending:
                fp = os.path.join(out_dir, p)
                os.makedirs(os.path.dirname(fp), exist_ok=True)
                with open(fp, "w", encoding="utf-8", newline="\n") as f:
                    f.write(t)
        self.pending = []
        return n


def update_data_map(repo, product, descriptor, written_by, note=""):
    """原 raw_writer 收尾：data_map.update_data_map(REPO, HF_TOKEN, ...)。"""
    from courier import data_map as dm
    return dm.update_data_map(repo, _token(), product, descriptor, written_by, note=note)


# ══ dataset_health_check 用（token 由呼叫端決定來源：環境變數或本機備援檔，見 resolve_health_token）══

def resolve_health_token() -> str:
    """原 dataset_health_check.main：os.getenv("HF_TOKEN") or env.get("HF_TOKEN","")（備援檔只在門內讀）。"""
    from courier import keys
    return os.getenv("HF_TOKEN") or keys.local_env_secret("HF_TOKEN")


class HealthDataset:
    """原 dataset_health_check 的 HF 讀取（token 留在物件內，不回給呼叫端）。"""

    def __init__(self, repo):
        self.repo = repo
        self._tok = resolve_health_token()

    def ready(self) -> bool:
        return bool(self._tok)

    def list_paths(self):
        from huggingface_hub import HfApi
        api = HfApi(token=self._tok)
        info = api.repo_info(repo_id=self.repo, repo_type="dataset", files_metadata=True)
        return set(s.rfilename for s in (info.siblings or []))

    def dl_json(self, path):
        from huggingface_hub import hf_hub_download
        try:
            local = hf_hub_download(repo_id=self.repo, filename=path, repo_type="dataset", token=self._tok)
        except Exception as e:
            if type(e).__name__ in ("EntryNotFoundError", "RepositoryNotFoundError") or "404" in str(e):
                return None
            raise
        with open(local, "r", encoding="utf-8") as f:
            return json.load(f)

    def last_csv_row(self, path):
        from huggingface_hub import hf_hub_download
        try:
            local = hf_hub_download(repo_id=self.repo, filename=path, repo_type="dataset", token=self._tok)
        except Exception:
            return None
        last = None
        with open(local, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    last = line
        return last


# ══ swing_lab/bootstrap 用 ══════════════════════════════════════════

def download_code_bundle(repo, code_dir, want):
    """原 bootstrap.main 的下載段（逐字）：want 空＝讀 latest.json（force_download）取指紋；回 (zip 路徑, 指紋)。
    缺 HF_TOKEN → SystemExit("缺 HF_TOKEN")（與原處一致）。"""
    from huggingface_hub import hf_hub_download
    tok = os.environ.get("HF_TOKEN", "").strip()
    if not tok:
        raise SystemExit("缺 HF_TOKEN")
    if not want:
        p = hf_hub_download(repo, code_dir + "/latest.json", repo_type="dataset", token=tok, force_download=True)
        with open(p, encoding="utf-8") as f:
            want = json.load(f)["sha256"]
    zp = hf_hub_download(repo, "%s/%s.zip" % (code_dir, want), repo_type="dataset", token=tok)
    return zp, want
