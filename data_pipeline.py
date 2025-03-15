import subprocess
import os

def run_multiple_scripts_in_venv(venv_path, script_list):
    """
    รันหลายไฟล์ Python ภายใน Virtual Environment (venv) ตามลำดับ
    
    Args:
        venv_path (str): Path ไปยังโฟลเดอร์ venv (เช่น "my_env")
        script_list (list): รายชื่อไฟล์ Python ที่ต้องการรัน (เช่น ["script1.py", "script2.py"])

    Returns:
        dict: แสดงผลลัพธ์ stdout และ stderr ของแต่ละสคริปต์
    """
    # ระบุ Python Interpreter ของ venv
    venv_python = os.path.join(venv_path, "bin", "python")  # macOS/Linux
    # venv_python = os.path.join(venv_path, "Scripts", "python.exe")  # Windows

    results = {}

    for script_path in script_list:
        print(f"Executing: {script_path} ...")
        try:
            # รัน Python สคริปต์
            result = subprocess.run([venv_python, script_path], capture_output=True, text=True, check=True)
            results[script_path] = {"stdout": result.stdout, "stderr": None}
            print(f"{script_path} executed successfully!\n")

        except subprocess.CalledProcessError as e:
            results[script_path] = {"stdout": e.stdout, "stderr": e.stderr}
            print(f"Error in {script_path}!\n")

    return results


if __name__ == "__main__":
    venv_path = ".venv"  # เปลี่ยนเป็น path ของ venv ที่ใช้งาน
    script_list = ["test_ingestion_staging_incremental.py", 
                   "test_ingestion_raw_incremental.py"]  # รายชื่อไฟล์ที่ต้องการรัน

    output = run_multiple_scripts_in_venv(venv_path, script_list)

    # แสดงผลลัพธ์ของแต่ละสคริปต์
    for script, result in output.items():
        print(f"Output from {script}:")
        print(result["stdout"])

        if result["stderr"]:
            print("Error:")
            print(result["stderr"])
