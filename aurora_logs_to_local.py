import boto3
import os
import datetime
import logging
import configparser
import json
import re

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_download_history(s3_client, bucket_name, instance_id):
    """
    从S3获取特定实例的下载历史记录
    
    参数:
    s3_client -- boto3 S3客户端
    bucket_name -- S3存储桶名称
    instance_id -- 数据库实例ID
    
    返回:
    下载历史记录字典，如果不存在则返回空字典
    """
    history_key = f"aurora-logs-history/{instance_id}/download-history.json"
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=history_key)
        history = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"已从S3加载实例 {instance_id} 的下载历史记录")
        return history
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"S3中不存在实例 {instance_id} 的下载历史记录，将创建新记录")
        return {'files': [], 'last_update': None}
    except Exception as e:
        logger.error(f"获取实例 {instance_id} 的下载历史记录时出错: {str(e)}")
        return {'files': [], 'last_update': None}

def update_download_history(s3_client, bucket_name, instance_id, history):
    """
    更新S3中特定实例的下载历史记录
    
    参数:
    s3_client -- boto3 S3客户端
    bucket_name -- S3存储桶名称
    instance_id -- 数据库实例ID
    history -- 下载历史记录字典
    """
    history_key = f"aurora-logs-history/{instance_id}/download-history.json"
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=history_key,
            Body=json.dumps(history, indent=2),
            ContentType='application/json'
        )
        logger.info(f"已更新实例 {instance_id} 的下载历史记录到S3")
    except Exception as e:
        logger.error(f"更新实例 {instance_id} 的下载历史记录时出错: {str(e)}")

def get_last_execution_time(s3_client, bucket_name):
    """
    从S3获取脚本最后执行时间
    """
    history_key = 'aurora-logs-history/last_execution.json'
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=history_key)
        history = json.loads(response['Body'].read().decode('utf-8'))
        last_execution_time = history.get('last_update')
        logger.info(f"已从S3加载脚本最后执行时间记录")
        return last_execution_time
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"S3中不存在执行时间记录，将创建新记录")
        return None
    except Exception as e:
        logger.error(f"获取执行时间记录时出错: {str(e)}")
        return None

def update_execution_time(s3_client, bucket_name):
    """
    更新S3中的脚本执行时间
    """
    history_key = 'aurora-logs-history/last_execution.json'
    try:
        history = {
            'last_update': datetime.datetime.now().isoformat()
        }
        s3_client.put_object(
            Bucket=bucket_name,
            Key=history_key,
            Body=json.dumps(history, indent=2),
            ContentType='application/json'
        )
        logger.info(f"已更新脚本执行时间记录到S3")
    except Exception as e:
        logger.error(f"更新执行时间记录时出错: {str(e)}")

def download_aurora_logs(rds_client, s3_client, db_instance_identifier, output_dir, bucket_name, config, days=7):
    """
    下载Aurora数据库实例的日志文件
    
    参数:
    rds_client -- boto3 RDS客户端
    s3_client -- boto3 S3客户端
    db_instance_identifier -- Aurora实例ID
    output_dir -- 日志文件下载目录
    bucket_name -- S3存储桶名称
    config -- 配置对象
    days -- 只下载最近几天的日志，默认为7天
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
        logger.info(f"找到 {len(log_files)} 个日志文件")
        
        # 计算几天前的日期
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
        current_date = datetime.datetime.now().date()
        logger.info(f"只下载 {cutoff_date.strftime('%Y-%m-%d')} 之后的日志文件")
        
        # 获取实例的下载历史
        instance_history = get_download_history(s3_client, bucket_name, db_instance_identifier)
        downloaded_files_history = instance_history.get('files', [])
        last_execution_time = instance_history.get('last_update')
        
        # 如果有上次执行时间，转换为时间戳
        last_execution_timestamp = None
        if last_execution_time:
            try:
                last_execution_datetime = datetime.datetime.fromisoformat(last_execution_time)
                last_execution_timestamp = int(last_execution_datetime.timestamp() * 1000)  # 转换为毫秒时间戳
                logger.info(f"上次脚本执行时间: {last_execution_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logger.warning(f"解析上次执行时间出错: {str(e)}")
        
        # 过滤符合日期条件的日志文件
        filtered_log_files = []
        for log_file in log_files:
            log_filename = log_file['LogFileName']
            file_size = log_file.get('Size', 0)
            last_written = log_file.get('LastWritten', 0)
            
            # 尝试从文件名中提取日期
            file_date = None
            try:
                # 查找文件名中的日期格式 YYYY-MM-DD
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', log_filename)
                if date_match:
                    date_str = date_match.group(1)
                    file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
                    
                    # 如果文件日期早于截止日期，则跳过
                    if file_date.date() < cutoff_date.date():
                        logger.info(f"跳过旧日志文件: {log_filename} (日期: {date_str})")
                        continue
            except Exception as e:
                # 如果无法解析日期，则默认下载
                logger.warning(f"无法从文件名 {log_filename} 解析日期: {str(e)}")
            
            # 如果有上次执行时间，检查文件是否在上次执行后被修改
            if last_execution_timestamp and last_written <= last_execution_timestamp:
                logger.info(f"跳过未修改的文件: {log_filename} (上次写入: {datetime.datetime.fromtimestamp(last_written/1000).strftime('%Y-%m-%d %H:%M:%S')})")
                continue
                
            filtered_log_files.append(log_file)
        
        logger.info(f"找到 {len(filtered_log_files)} 个需要下载的文件")
        
        downloaded_files = []
        
        # 下载每个日志文件
        for log_file in filtered_log_files:
            log_filename = log_file['LogFileName']
            file_size = log_file.get('Size', 0)
            last_written = log_file.get('LastWritten', 0)
            
            # 检查文件是否已下载过且内容未变化
            file_already_downloaded = False
            for history_file in downloaded_files_history:
                if history_file['filename'] == log_filename and history_file['last_written'] == last_written:
                    logger.info(f"跳过已下载且未修改的文件: {log_filename}")
                    file_already_downloaded = True
                    break
            
            if file_already_downloaded:
                continue
            
            local_filename = os.path.join(output_dir, os.path.basename(log_filename))
            logger.info(f"下载日志文件: {log_filename} (大小: {file_size} 字节, 上次写入: {datetime.datetime.fromtimestamp(last_written/1000).strftime('%Y-%m-%d %H:%M:%S')})")
            
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
            
            # 更新或添加下载历史
            updated = False
            for i, history_file in enumerate(downloaded_files_history):
                if history_file['filename'] == log_filename:
                    downloaded_files_history[i] = {
                        'filename': log_filename,
                        'size': file_size,
                        'last_written': last_written,
                        'download_time': datetime.datetime.now().isoformat()
                    }
                    updated = True
                    break
            
            if not updated:
                downloaded_files_history.append({
                    'filename': log_filename,
                    'size': file_size,
                    'last_written': last_written,
                    'download_time': datetime.datetime.now().isoformat()
                })
            
            logger.info(f"已下载到: {local_filename}")
        
        # 更新实例的下载历史
        instance_history['files'] = downloaded_files_history
        instance_history['last_update'] = datetime.datetime.now().isoformat()
        
        # 更新S3中的下载历史
        update_download_history(s3_client, bucket_name, db_instance_identifier, instance_history)
            
        return downloaded_files
        
    except Exception as e:
        logger.error(f"下载Aurora日志时出错: {str(e)}")
        raise

def main():
    # 读取配置文件
    config = configparser.ConfigParser()
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    
    if not os.path.exists(config_file):
        logger.error(f"配置文件不存在: {config_file}")
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    config.read(config_file)
    
    # 从配置文件获取AWS配置
    region_name = config.get('aws', 'region_name')
    
    # 获取S3配置
    s3_bucket = config.get('aws', 's3_bucket_name')
    
    # 获取多个实例ID（支持多行格式）
    instance_config = config.get('instances', 'db_instance_identifiers')
    # 分割多行配置并移除空行和空格
    db_instance_identifiers = [line.strip() for line in instance_config.splitlines() if line.strip()]
    
    if not db_instance_identifiers:
        logger.error("配置文件中未找到有效的实例ID")
        raise ValueError("配置文件中未找到有效的实例ID")
    
    # 获取输出目录
    output_base_dir = config.get('local', 'output_dir', fallback='/tmp/aurora-logs')
    
    logger.info(f"从配置文件加载配置: region={region_name}, 实例数量={len(db_instance_identifiers)}, S3存储桶={s3_bucket}")
    
    # 创建AWS客户端
    rds_client = boto3.client('rds', region_name=region_name)
    s3_client = boto3.client('s3', region_name=region_name)
    
    # 处理每个实例
    for db_instance_identifier in db_instance_identifiers:
        logger.info(f"开始处理实例: {db_instance_identifier}")
        
        # 本地输出目录
        output_dir = os.path.join(output_base_dir, db_instance_identifier)
        
        try:
            # 下载Aurora日志（只下载最近7天的）
            logger.info(f"开始下载实例 {db_instance_identifier} 最近7天的日志")
            downloaded_files = download_aurora_logs(
                rds_client, 
                s3_client,
                db_instance_identifier, 
                output_dir,
                s3_bucket,
                config,
                days=7
            )
            
            if not downloaded_files:
                logger.info(f"实例 {db_instance_identifier} 没有需要下载的日志文件")
                continue
                
            logger.info(f"实例 {db_instance_identifier}: 成功下载 {len(downloaded_files)} 个文件到 {output_dir}")
            
        except Exception as e:
            logger.error(f"处理实例 {db_instance_identifier} 时出错: {str(e)}")
            # 继续处理下一个实例，而不是中断整个程序
            continue

    # 所有实例处理完成后，更新执行时间
    update_execution_time(s3_client, s3_bucket)
    logger.info("脚本执行完成")

if __name__ == "__main__":
    main()