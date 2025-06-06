import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


def setup_logger(name: str, level=logging.INFO, max_bytes=5 * 1024 * 1024, backup_count=5):
    """
    配置并返回一个支持日志滚动的 logger 实例，默认日志文件路径为 logs/<logger_name>.log。

    参数:
        name (str): logger 的名称。
        level (int, optional): 日志记录级别。
        max_bytes (int, optional): 单个日志文件的最大大小（字节），默认5MB。
        backup_count (int, optional): 保留的备份日志文件数量，默认5个。

    返回:
        logging.Logger: 配置好的 logger 实例。
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"
    )
    # 控制台 handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(console_handler)

    # 自动创建 logs 子目录
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{name}.log")

    # 文件 handler
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# 确保logger模块存在并正确导入
logger = setup_logger("stock_strategy")
