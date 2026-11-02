"""Xuất các bảng đặc trưng trung gian của notebook huấn luyện ra Parquet, làm đáp án chuẩn để kiểm tra code Spark.

Chạy ngoài Docker bằng môi trường conda của notebook:  make reference
Thực thi các cell của training/main.ipynb trong bộ nhớ (không sửa file notebook). Riêng cell tách Cash/Revolving
bị đổi một dòng ở bản chạy trong bộ nhớ để giữ lại SK_ID_CURR, nhờ đó khớp được từng hồ sơ.
Cần khoảng 6 GB RAM, nên tắt các container trước khi chạy.

Đầu ra (trong work/reference):
  application_features.parquet     bảng application đã có đặc trưng và one-hot, trước khi ghép bureau
  bureau_balance_features.parquet  mỗi khoản vay một dòng
  bureau_features.parquet          mỗi khách hàng một dòng, trước khi điền giá trị thiếu
  application_train_cash.parquet   bảng cuối của notebook: SK_ID_CURR, TARGET và 221 đặc trưng
"""
import argparse
import json
import os
import sys
import time

os.environ.setdefault("MPLBACKEND", "Agg")      # vẽ biểu đồ nhưng không mở cửa sổ
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from credit_pipeline.logs import run  # noqa: E402

PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
NOTEBOOK_DIR = os.path.join(PIPELINE_DIR, "..", "training")
LAST_CELL_BEFORE_SPLIT = 40   # cell 41 ghép bureau vào application, tách Cash/Revolving rồi xóa các bảng trung gian
SPLIT_CELL = 41
DROP_SPLIT_LINE = 'drop_split_cols = ["NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans", "SK_ID_CURR"]'
KEEP_ID_LINE = 'drop_split_cols = ["NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans"]'


def main(log):
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PIPELINE_DIR, "work", "reference"))
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    os.chdir(NOTEBOOK_DIR)      # notebook đọc dữ liệu theo đường dẫn tương đối từ thư mục của nó

    with open("main.ipynb", encoding="utf-8") as notebook_file:
        cells = json.load(notebook_file)["cells"]

    namespace = {"__name__": "__notebook__"}
    from IPython.display import display   # một số cell gọi display()
    namespace["display"] = display

    def run_cell(index, source=None):
        code = source if source is not None else "".join(cells[index]["source"])
        started = time.time()
        exec(compile(code, f"main.ipynb[cell {index}]", "exec"), namespace)
        log(f"cell {index} xong ({time.time() - started:.1f}s)")

    log(f"Đang chạy cell 0 đến {LAST_CELL_BEFORE_SPLIT} của notebook")
    for index in range(LAST_CELL_BEFORE_SPLIT + 1):
        if cells[index]["cell_type"] == "code":
            run_cell(index)

    for key, file_name in (("application_train", "application_features"), ("bureau_balance_features", "bureau_balance_features"),
                           ("bureau_features", "bureau_features")):
        namespace[key].to_parquet(os.path.join(args.out_dir, f"{file_name}.parquet"), index=False)
        log(f"Đã ghi {file_name}.parquet {namespace[key].shape}")

    log("Đang chạy cell ghép bureau và tách Cash/Revolving")
    split_source = "".join(cells[SPLIT_CELL]["source"])
    if DROP_SPLIT_LINE not in split_source:
        raise RuntimeError("Cell tách Cash/Revolving của notebook đã đổi, cần cập nhật DROP_SPLIT_LINE")
    run_cell(SPLIT_CELL, split_source.replace(DROP_SPLIT_LINE, KEEP_ID_LINE))

    cash = namespace["application_train_cash"]
    cash.insert(0, "SK_ID_CURR", cash.pop("SK_ID_CURR"))
    cash.to_parquet(os.path.join(args.out_dir, "application_train_cash.parquet"), index=False)
    feature_count = cash.shape[1] - 2   # trừ SK_ID_CURR và TARGET
    log(f"Đã ghi application_train_cash.parquet: {cash.shape[0]:,} dòng, {feature_count} đặc trưng")
    if cash.shape[0] != 278220 or feature_count != 221:
        raise RuntimeError("Số dòng hoặc số đặc trưng không đúng như notebook")
    log(f"Hoàn tất: 4 file trong {args.out_dir}")


if __name__ == "__main__":
    run("export_reference", main)
