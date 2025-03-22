import boto3
import os
import datetime
import logging
import configparser
import json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_aurora_logs(rds_client, db_instance_identifier, output_dir, days=7, upload_record=None, s3_prefix=None):
    """
    下载Aurora数据库实例的日志文件
    
    参数:
    rds_client -- boto3 RDS客户端
    db_instance_identifier -- Aurora实例ID
    output_dir -- 日志文件下载目录
    days -- 只下载最近几天的日志，默认为7天
    upload_record -- 已上传文件的记录
    s3_prefix -- S3前缀，用于构建S3键
    """
    try:
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 获取可用的日志文件列表
        response = rds_client.describe_db_log_files(
            DBInstanceIdentifier=db_instance_identifier
        )
        
        log_files = response.get('DescribeDBLogFiles', [])
        logger.info(f"found {len(log_files)} log files")
        
        # 计算7天前的日期
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
        current_date = datetime.datetime.now().date()
        logger.info(f"only download after {cutoff_date.strftime('%Y-%m-%d')} log files")
        
        downloaded_files = []
        skipped_files = []
        
        # 下载每个日志文件
        for log_file in log_files:
            log_filename = log_file['LogFileName']
            file_size = log_file.get('Size', 0)
            
            # 尝试从文件名中提取日期
            file_date = None
            is_current_day_log = False
            try:
                # 查找文件名中的日期格式 YYYY-MM-DD
                import re
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', log_filename)
                if date_match:
                    date_str = date_match.group(1)
                    file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
                    
                    # 如果文件日期早于截止日期，则跳过
                    if file_date.date() < cutoff_date.date():
                        logger.info(f"skip old log files: {log_filename} (Date: {date_str})")
                        continue
                    
                    # 检查是否为当天日志
                    is_current_day_log = (file_date.date() == current_date)
                else:
                    # 如果文件名中没有日期，假定为当前日志
                    is_current_day_log = True
            except Exception as e:
                # 如果无法解析日期，则默认为当前日志
                logger.warning(f"Unable to parse date from filename {log_filename}: {str(e)}")
                is_current_day_log = True
            
            # 检查是否已上传且非当日日志
            if upload_record and s3_prefix and not is_current_day_log:
                base_filename = os.path.basename(log_filename)
                s3_key = f"{s3_prefix}/{base_filename}"
                
                if s3_key in upload_record:
                    logger.info(f"Skipping previously uploaded non-current-day log file: {log_filename}")
                    skipped_files.append(upload_record[s3_key])
                    continue
            
            local_filename = os.path.join(output_dir, os.path.basename(log_filename))
            logger.info(f"Downloading log file: {log_filename} (size: {file_size} bytes)")
            
            # 获取日志文件内容
            log_content = ""
            marker = '0'
            
            while True:
                log_response = rds_client.download_db_log_file_portion(
                    DBInstanceIdentifier=db_instance_identifier,
                    LogFileName=log_filename,
                    Marker=marker
                )
                
                log_data = log_response.get('LogFileData', '')
                log_content += log_data
                
                # 检查是否有更多数据
                if not log_response.get('AdditionalDataPending', False):
                    break
                    
                marker = log_response.get('Marker', '0')
            
            # 写入本地文件
            with open(local_filename, 'w') as f:
                f.write(log_content)
                
            downloaded_files.append(local_filename)
            logger.info(f"Downloaded to: {local_filename}")
            
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Error downloading Aurora logs: {str(e)}")
        raise

def get_upload_record_from_s3(s3_client, bucket_name, record_key, local_record_file):
    """
    从S3获取上传记录文件
    """
    try:
        # 检查S3上是否存在记录文件
        s3_client.head_object(Bucket=bucket_name, Key=record_key)
        
        # 从S3下载记录文件
        s3_client.download_file(bucket_name, record_key, local_record_file)
        logger.info(f"Upload record downloaded from S3: s3://{bucket_name}/{record_key}")
        
        with open(local_record_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.info(f"Failed to get upload record from S3, will use local record: {str(e)}")
        # 如果从S3获取失败，使用本地记录
        return get_upload_record(local_record_file)

def get_upload_record(record_file):
    """
    获取已上传文件的记录
    """
    if os.path.exists(record_file):
        try:
            with open(record_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"读取上传记录文件失败: {str(e)}")
    return {}

def save_upload_record(record_file, record):
    """
    保存上传记录到本地
    """
    try:
        with open(record_file, 'w') as f:
            json.dump(record, f)
    except Exception as e:
        logger.warning(f"Failed to save upload record file: {str(e)}")

def save_upload_record_to_s3(s3_client, bucket_name, record_key, record_file, record):
    """
    保存上传记录到S3
    """
    try:
        # 先保存到本地
        save_upload_record(record_file, record)
        
        # 然后上传到S3
        s3_client.upload_file(record_file, bucket_name, record_key)
        logger.info(f"Upload record saved to S3: s3://{bucket_name}/{record_key}")
    except Exception as e:
        logger.warning(f"Failed to save upload record to S3: {str(e)}")

def is_active_log_file(log_filename):
    """
    判断是否为活跃日志文件（未滚动的日志文件）
    通常活跃日志文件名中不包含日期，或者包含当前日期
    """
    try:
        # 检查文件名是否包含日期
        import re
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', log_filename)
        
        # 如果不包含日期，可能是活跃日志文件
        if not date_match:
            return True
            
        # 如果包含日期，检查是否为当前日期
        date_str = date_match.group(1)
        file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        current_date = datetime.datetime.now().date()
        
        # 如果是当天的日志，视为活跃日志文件
        return file_date == current_date
    except Exception:
        # 如果无法解析，默认为非活跃文件
        return False

def upload_to_s3(s3_client, file_path, bucket_name, s3_prefix=None, upload_record=None, record_file=None):
    """
    将文件上传到S3，支持断点续传
    对于活跃日志文件，每次都覆盖上传
    """
    try:
        file_name = os.path.basename(file_path)
        
        # 构建S3对象键
        if s3_prefix:
            s3_key = f"{s3_prefix}/{file_name}"
        else:
            s3_key = file_name
        
        # 判断是否为活跃日志文件
        active_log = is_active_log_file(file_name)
        
        # 检查是否已上传（对于非活跃日志文件）
        if not active_log and upload_record and s3_key in upload_record:
            logger.info(f"File already uploaded, skipping: {s3_key}")
            return upload_record[s3_key]
        
        # 对于活跃日志文件，显示覆盖上传信息
        if active_log:
            logger.info(f"Uploading active log file to S3 (overwrite mode): {file_path} -> s3://{bucket_name}/{s3_key}")
        else:
            logger.info(f"Uploading file to S3: {file_path} -> s3://{bucket_name}/{s3_key}")
        
        # 上传文件到S3
        s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=s3_key
        )
        
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        logger.info(f"Upload successful: {s3_uri}")
        
        # 更新上传记录
        if upload_record is not None:
            upload_record[s3_key] = s3_uri
            if record_file:
                save_upload_record(record_file, upload_record)
                
        return s3_uri
        
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

def main():
    # 读取配置文件
    config = configparser.ConfigParser()
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    
    if not os.path.exists(config_file):
        logger.error(f"Configuration file does not exist: {config_file}")
        raise FileNotFoundError(f"Configuration file does not exist: {config_file}")
    
    config.read(config_file)
    
    # 从配置文件获取AWS配置
    region_name = config.get('aws', 'region_name')
    s3_bucket_name = config.get('aws', 's3_bucket_name')
    
    # 获取多个实例ID（支持多行格式）
    instance_config = config.get('instances', 'db_instance_identifiers')
    # 分割多行配置并移除空行和空格
    db_instance_identifiers = [line.strip() for line in instance_config.splitlines() if line.strip()]
    
    if not db_instance_identifiers:
        logger.error("No valid instance ID found in configuration file")
        raise ValueError("No valid instance ID found in configuration file")
    
    logger.info(f"Configuration loaded from config file: region={region_name}, number of instances={len(db_instance_identifiers)}")
    
    # 创建AWS客户端
    rds_client = boto3.client('rds', region_name=region_name)
    s3_client = boto3.client('s3', region_name=region_name)
    
    # 处理每个实例
    for db_instance_identifier in db_instance_identifiers:
        logger.info(f"start handling instance: {db_instance_identifier}")
        
        # 创建带有当前日期的S3前缀
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        s3_prefix = f"aurora-logs/{db_instance_identifier}/{current_date}"
        
        # 本地临时目录
        temp_dir = f"/tmp/aurora-logs-{db_instance_identifier}"
        
        # 上传记录文件
        record_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'upload_records')
        if not os.path.exists(record_dir):
            os.makedirs(record_dir)
        record_file = os.path.join(record_dir, f"{db_instance_identifier}_{current_date}.json")
        
        # S3上的记录文件路径
        record_key = f"aurora-logs-records/{db_instance_identifier}_{current_date}.json"
        
        # 从S3获取上传记录
        upload_record = get_upload_record_from_s3(s3_client, s3_bucket_name, record_key, record_file)
        logger.info(f"Upload record loaded, number of uploaded files: {len(upload_record)}")
        
        try:
            # 下载Aurora日志（只下载最近7天的，跳过已上传的非当日日志）
            logger.info(f"start download instance: {db_instance_identifier} download logs in 7 days")
            downloaded_files = download_aurora_logs(
                rds_client, 
                db_instance_identifier, 
                temp_dir, 
                days=7,
                upload_record=upload_record,
                s3_prefix=s3_prefix
            )
            
            if not downloaded_files:
                logger.info(f"Instance {db_instance_identifier} does not have any log files to download or upload")
                continue
            
            # 上传日志到S3（活跃日志文件会覆盖上传）
            uploaded_files = []
            for file_path in downloaded_files:
                s3_uri = upload_to_s3(s3_client, file_path, s3_bucket_name, s3_prefix, 
                                      upload_record=upload_record, record_file=record_file)
                uploaded_files.append(s3_uri)
                
            # 将最终的上传记录保存到S3
            save_upload_record_to_s3(s3_client, s3_bucket_name, record_key, record_file, upload_record)
                
            logger.info(f"Instance {db_instance_identifier}: success uploaded {len(uploaded_files)} to S3")
            
            # 清理临时文件
            for file_path in downloaded_files:
                os.remove(file_path)
            
            logger.info(f"Instance {db_instance_identifier}: temp files cleaned up")
            
        except Exception as e:
            logger.error(f"handling instance {db_instance_identifier} errors: {str(e)}")
            # 即使出错，也保存当前的上传记录到S3
            save_upload_record_to_s3(s3_client, s3_bucket_name, record_key, record_file, upload_record)
            # 继续处理下一个实例，而不是中断整个程序
            continue

if __name__ == "__main__":
    main()