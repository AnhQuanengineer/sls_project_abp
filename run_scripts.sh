#!/bin/bash
# run_scripts.sh

PYTHON=/home/abp-server4/anaconda3/bin/python3

SCRIPT_MONGO_NOT_CLASSIFY=/home/abp-server4/sls_project_abp/spark/mongo_spark_not_classify_org_id.py
SCRIPT_MONGO_CLASSIFY=/home/abp-server4/sls_project_abp/spark/mongo_spark_classify_org_id.py
SCRIPT_POSTGRES=/home/abp-server4/sls_project_abp/spark/postgres_spark.py

# Lấy ngày hiện tại theo định dạng YYYY-MM-DD
CURRENT_DATE=$(date +%Y-%m-%d)
LOG_DIR=/home/abp-server4/sls_project_abp/logs/$CURRENT_DATE

# Tạo thư mục log nếu chưa tồn tại
mkdir -p $LOG_DIR

# Định nghĩa file log riêng cho từng script
LOGFILE_MONGO_NOT_CLASSIFY=$LOG_DIR/mongo_spark_not_classify.log
LOGFILE_MONGO_CLASSIFY=$LOG_DIR/mongo_spark_classify.log
LOGFILE_POSTGRES=$LOG_DIR/postgres_spark.log

# Ghi thời gian bắt đầu vào cả ba log
echo "=== Run at $(date) ===" >> $LOGFILE_MONGO_NOT_CLASSIFY
echo "=== Run at $(date) ===" >> $LOGFILE_MONGO_CLASSIFY
echo "=== Run at $(date) ===" >> $LOGFILE_POSTGRES

# Chạy script MONGO_NOT_CLASSIFY và ghi log
echo "Running Script MONGO_NOT_CLASSIFY: $SCRIPT_MONGO_NOT_CLASSIFY" >> $LOGFILE_MONGO_NOT_CLASSIFY
$PYTHON $SCRIPT_MONGO_NOT_CLASSIFY >> $LOGFILE_MONGO_NOT_CLASSIFY 2>&1

# Kiểm tra nếu script MONGO_NOT_CLASSIFY chạy thành công
if [ $? -eq 0 ]; then
    echo "Script MONGO_NOT_CLASSIFY completed successfully" >> $LOGFILE_MONGO_NOT_CLASSIFY
    # Chạy script MONGO_CLASSIFY và ghi log
    echo "Running Script MONGO_CLASSIFY: $SCRIPT_MONGO_CLASSIFY" >> $LOGFILE_MONGO_CLASSIFY
    $PYTHON $SCRIPT_MONGO_CLASSIFY >> $LOGFILE_MONGO_CLASSIFY 2>&1
    if [ $? -eq 0 ]; then
        echo "Script MONGO_CLASSIFY completed successfully" >> $LOGFILE_MONGO_CLASSIFY
        # Chạy script POSTGRES và ghi log
        echo "Running Script POSTGRES: $SCRIPT_POSTGRES" >> $LOGFILE_POSTGRES
        $PYTHON $SCRIPT_POSTGRES >> $LOGFILE_POSTGRES 2>&1
        if [ $? -eq 0 ]; then
            echo "Script POSTGRES completed successfully" >> $LOGFILE_POSTGRES
        else
            echo "Script POSTGRES failed" >> $LOGFILE_POSTGRES
        fi
    else
        echo "Script MONGO_CLASSIFY failed, skipping Script POSTGRES" >> $LOGFILE_MONGO_CLASSIFY
    fi
else
    echo "Script MONGO_NOT_CLASSIFY failed, skipping subsequent scripts" >> $LOGFILE_MONGO_NOT_CLASSIFY
fi

# Ghi thời gian kết thúc vào cả ba log
echo "=== End at $(date) ===" >> $LOGFILE_MONGO_NOT_CLASSIFY
echo "=== End at $(date) ===" >> $LOGFILE_MONGO_CLASSIFY
echo "=== End at $(date) ===" >> $LOGFILE_POSTGRES
echo "" >> $LOGFILE_MONGO_NOT_CLASSIFY
echo "" >> $LOGFILE_MONGO_CLASSIFY
echo "" >> $LOGFILE_POSTGRES