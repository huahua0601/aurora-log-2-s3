# Aurora日志同步工具

## 简介

Aurora日志同步工具是一个用于自动下载Amazon Aurora数据库日志并上传到S3存储桶的Python脚本。该工具支持多个Aurora实例，并具有断点续传功能，可以有效避免重复下载和上传已处理过的日志文件。

## 功能特点

- 支持多个Aurora数据库实例
- 自动下载最近指定天数（默认7天）的日志文件
- 将日志文件上传到指定的S3存储桶
- 智能处理活跃日志文件（当天日志）和历史日志文件
- 断点续传功能，避免重复处理已上传的历史日志文件
- 上传记录保存在S3，确保即使本地记录丢失也能恢复上传状态

## 安装要求

- Python 3.6+
- AWS SDK for Python (Boto3)
- 有权限访问Aurora实例和S3存储桶的AWS凭证

## 安装步骤

1. 克隆或下载本仓库到本地

2. 安装所需依赖：

```bash
pip install boto3
```
3. 配置AWS凭证：
- 使用AWS CLI配置AWS凭证，或者手动创建一个名为`~/.aws/credentials`的文件，并添加以下内容：
```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```
## 配置说明
- `config.ini`文件中包含了工具的配置信息，包括Aurora实例ID、S3存储桶名称等。
- `uploaded_records.json`文件用于记录已上传的日志文件信息，避免重复处理。
- `log`目录用于存储日志文件。

```
[aws]
region_name = us-east-1
s3_bucket_name = your-s3-bucket-name

[instances]
db_instance_identifiers = 
    instance-id-1
    instance-id-2
    instance-id-3
```
配置项说明：

- region_name ：AWS区域名称
- s3_bucket_name ：用于存储日志的S3存储桶名称
- db_instance_identifiers ：Aurora实例ID列表，每行一个实例ID
  
## 使用方法
1. 运行脚本：
```bash
python aurora_logs_to_s3.py
```
2. 脚本会自动下载Aurora实例的日志文件，并将其上传到指定的S3存储桶。
3. 日志文件将按照实例ID和日期进行分类存储。
4. 上传记录将保存在`uploaded_records.json`文件中，以确保断点续传功能。
5. 建议设置定时任务，定期执行脚本：
```bash
# 示例：每小时执行一次
0 * * * * cd /path/to/script && python aurora_logs_to_s3.py >> /path/to/logfile.log 2>&1
```
## 工作原理
1. 脚本读取配置文件，获取AWS区域、S3存储桶名称和Aurora实例ID列表
2. 对于每个Aurora实例：
   - 从S3获取之前的上传记录（如果存在）
   - 获取实例的日志文件列表
   - 下载最近7天的日志文件（跳过已上传的非当天日志文件）
   - 将日志文件上传到S3：
     - 当天的活跃日志文件：每次都覆盖上传
     - 历史日志文件：如果已上传则跳过
   - 更新上传记录并保存到S3
   - 清理临时文件


## 日志文件存储结构
日志文件在S3中的存储路径格式为：

```plaintext
s3://your-bucket-name/aurora-logs/instance-id/YYYY-MM-DD/log-filename
 ```

上传记录文件在S3中的存储路径格式为：

```plaintext
s3://your-bucket-name/aurora-logs-records/instance-id_YYYY-MM-DD.json
 ```

## 故障排查
- 如果脚本无法连接到AWS服务，请检查AWS凭证和网络连接
- 如果无法下载日志文件，请确保IAM用户/角色有权限访问RDS服务和相关API
- 如果无法上传到S3，请确保IAM用户/角色有权限访问S3存储桶
- 查看脚本输出的日志信息，了解详细的错误原因
## 所需IAM权限
脚本需要以下最小IAM权限才能正常工作：

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds:DescribeDBLogFiles",
                "rds:DownloadDBLogFilePortion"
            ],
            "Resource": "arn:aws:rds:*:*:db:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
 ```

请将 your-bucket-name 替换为您实际使用的S3存储桶名称。

## 许可证
MIT License

Copyright (c) 2023

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.