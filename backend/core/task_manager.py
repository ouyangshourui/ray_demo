"""
任务管理器
统一管理所有异步任务的状态、进度、日志和结果
"""
import uuid
import threading
from datetime import datetime


class TaskManager:
    """线程安全的任务管理器"""

    def __init__(self):
        self._tasks = {}
        self._lock = threading.Lock()

    def create_task(self, task_type: str, meta: dict = None) -> str:
        """创建新任务，返回 task_id"""
        task_id = str(uuid.uuid4())
        with self._lock:
            self._tasks[task_id] = {
                "id": task_id,
                "type": task_type,
                "status": "running",
                "progress": 0,
                "logs": [],
                "result": None,
                "error": None,
                "meta": meta or {},
                "start_time": datetime.now().isoformat(),
            }
        return task_id

    def log(self, task_id: str, message: str):
        """追加一条日志"""
        with self._lock:
            if task_id in self._tasks:
                ts = datetime.now().strftime("%H:%M:%S")
                self._tasks[task_id]["logs"].append(f"[{ts}] {message}")

    def set_progress(self, task_id: str, progress: int):
        """更新进度"""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["progress"] = progress

    def complete(self, task_id: str, result: dict):
        """标记任务完成"""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["status"] = "completed"
                self._tasks[task_id]["progress"] = 100
                self._tasks[task_id]["result"] = result

    def fail(self, task_id: str, error: str):
        """标记任务失败"""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["status"] = "failed"
                self._tasks[task_id]["error"] = error

    def get(self, task_id: str):
        """获取任务详情"""
        with self._lock:
            return self._tasks.get(task_id)

    def list_all(self):
        """列出所有任务摘要"""
        with self._lock:
            return [
                {
                    "id": t["id"],
                    "type": t["type"],
                    "status": t["status"],
                    "progress": t["progress"],
                    "start_time": t["start_time"],
                }
                for t in self._tasks.values()
            ]

    def run_async(self, task_id: str, target, *args):
        """在后台线程执行任务函数"""
        def wrapper():
            try:
                target(task_id, *args)
            except Exception as e:
                import traceback
                self.fail(task_id, str(e))
                self.log(task_id, f"错误: {traceback.format_exc()}")

        thread = threading.Thread(target=wrapper)
        thread.daemon = True
        thread.start()


# 全局单例
task_manager = TaskManager()
