import os
import hashlib
import json
import requests
from urllib.parse import urljoin
import sys
import asyncio
from upinfopy import UpInfo, Api78

class FileUpdater:
    def __init__(self, local_directory, remote_base_url, api):
        self.local_directory = local_directory
        self.remote_base_url = remote_base_url
        self.remote_md5_url = urljoin(remote_base_url, "md5.json")
        self.api = api

    def calculate_file_md5(self, filepath):
        """计算文件的MD5哈希值"""
        md5_hash = hashlib.md5()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()

    def get_file_list_with_md5(self, directory):
        """获取指定目录下所有文件的列表及其MD5值"""
        file_list = {}
        for root, dirs, files in os.walk(directory):
            # 跳过 __pycache__ 目录
            dirs[:] = [d for d in dirs if d != '__pycache__']
            for file in files:
                filepath = os.path.join(root, file)
                relative_path = os.path.relpath(filepath, directory).replace("\\", "/")
                file_list[relative_path] = self.calculate_file_md5(filepath)
        return file_list

    def save_md5_json(self, output_file="md5.json"):
        """生成并保存包含文件MD5值的JSON文件"""
        md5_list = self.get_file_list_with_md5(self.local_directory)
        output_path = os.path.join(self.local_directory, output_file)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(md5_list, f, ensure_ascii=False, indent=2)
        return output_path

    async def get_signed_url(self, hashname):
        """通过API获取带有签名的URL"""
        up = UpInfo.getMaster()
        up.set_par(hashname)
        response = await self.api.send_back("apisimple/mes/mes_message/generateSignedUrl", up.to_url_encode())
        return response
 

    async def download_file(self, hashname, filepath):
        """下载文件到指定路径"""
        try:
            signed_url = await self.get_signed_url(hashname)
            response = requests.get(signed_url)
            response.raise_for_status()
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            print(f"下载文件时出错：{str(e)}")
            return

    async def update_files(self):
        """根据远程MD5文件列表更新本地文件"""
        try:
            signed_url = await self.get_signed_url(self.remote_md5_url)
            # 下载远程MD5文件列表
            response = requests.get(signed_url)
            response.raise_for_status()
            remote_md5_list = response.json()
            
            # 获取本地文件MD5列表
            local_md5_list = self.get_file_list_with_md5(self.local_directory)
            
            updated_files = []
            # 比较并更新文件
            for filepath, remote_md5 in remote_md5_list.items():
                local_filepath = os.path.join(self.local_directory, filepath)
                if filepath not in local_md5_list or local_md5_list[filepath] != remote_md5:
                    print(f"正在更新文件: {filepath}")
                    await self.download_file(self.remote_base_url+filepath, local_filepath)
                    updated_files.append(filepath)
            
            # 更新本地MD5文件
            local_md5_file = self.save_md5_json()
            
            return json.dumps({
                "status": "success",
                "updated_files": updated_files,
                "local_md5_file": local_md5_file
            }, ensure_ascii=False)
        except Exception as e:
            return json.dumps({
                "status": "error",
                "message": str(e)
            }, ensure_ascii=False)

    def initialize_local_directory(self):
        """初始化本地目录"""
        try:
            # 确保本地目录存在
            os.makedirs(self.local_directory, exist_ok=True)
            
            # 生成本地md5.json文件
            local_md5_file = self.save_md5_json()
            
            print(f"已生成 md5.json 文件：{local_md5_file}")
            
            return json.dumps({
                "status": "success",
                "message": f"本地目录 '{self.local_directory}' 已初始化",
                "md5_file": local_md5_file,
                "md5_content": self.get_file_list_with_md5(self.local_directory)
            }, ensure_ascii=False)
        except Exception as e:
            print(f"初始化本地目录时出错：{str(e)}")
            return json.dumps({
                "status": "error",
                "message": str(e)
            }, ensure_ascii=False)

# 用于从C#调用的函数
def update_files(local_directory, remote_base_url, api):
    updater = FileUpdater(local_directory, remote_base_url, api)
    return asyncio.run(updater.update_files())

# 新增：用于从C#调用初始化函数
def initialize_directory(local_directory, remote_base_url, api):
    try:
        print(f"正在初始化目录：{local_directory}")
        print(f"远程基础URL：{remote_base_url}")
        updater = FileUpdater(local_directory, remote_base_url, api)
        result = updater.initialize_local_directory()
        print(f"初始化结果：{result}")
        return result
    except Exception as e:
        print(f"初始化过程中出现异常：{str(e)}")
        return json.dumps({
            "status": "error",
            "message": str(e)
        }, ensure_ascii=False)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python ./src/basic/file_updater.py initialize_directory ./dist http://down.778878.net/download/scan78/")
        sys.exit(1)
    
    command = sys.argv[1]
    if command == "initialize_directory":
        result = initialize_directory(sys.argv[2], sys.argv[3], Api78("http://api.778878.net"))
        print(result)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)