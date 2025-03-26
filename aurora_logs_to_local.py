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

def download_aurora_logs(rds_client, db_instance_identifier, output_dir, days=1):
    """
    下载Aurora数据库实例的日志文件
    
    参数:
    rds_client -- boto3 RDS客户端
    db_instance_identifier -- Aurora实例ID
    output_dir -- 日志文件下载目录
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
        
        # 计算7天前的日期
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
        current_date = datetime.datetime.now().date()
        logger.info(f"只下载 {cutoff_date.strftime('%Y-%m-%d')} 之后的日志文件")
        
        downloaded_files = []
        
        # 下载每个日志文件
        for log_file in log_files:
            log_filename = log_file['LogFileName']
            file_size = log_file.get('Size', 0)
            
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
            
            local_filename = os.path.join(output_dir, os.path.basename(log_filename))
            logger.info(f"下载日志文件: {log_filename} (大小: {file_size} 字节)")
            
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
            logger.info(f"已下载到: {local_filename}")
            
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
    
    # 获取多个实例ID（支持多行格式）
    instance_config = config.get('instances', 'db_instance_identifiers')
    # 分割多行配置并移除空行和空格
    db_instance_identifiers = [line.strip() for line in instance_config.splitlines() if line.strip()]
    
    if not db_instance_identifiers:
        logger.error("配置文件中未找到有效的实例ID")
        raise ValueError("配置文件中未找到有效的实例ID")
    
    # 获取输出目录
    output_base_dir = config.get('local', 'output_dir', fallback='/tmp/aurora-logs')
    
    logger.info(f"从配置文件加载配置: region={region_name}, 实例数量={len(db_instance_identifiers)}")
    
    # 创建AWS客户端
    rds_client = boto3.client('rds', region_name=region_name)
    
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
                db_instance_identifier, 
                output_dir, 
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

if __name__ == "__main__":
    main()