import redis
import json
import pickle
from typing import Any, Callable, List, Optional
from abc import ABC, abstractmethod


class Task:
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.id = f"task_{id(self)}"

    def execute(self, input_data: Any = None) -> Any:
        if input_data is not None:
            return self.func(input_data, *self.args, **self.kwargs)
        return self.func(*self.args, **self.kwargs)

    def __repr__(self):
        return f"Task({self.func.__name__})"


class Group:
    def __init__(self, *tasks: Task):
        self.tasks = tasks

    def execute_all(self, input_data: Any = None) -> List[Any]:
        results = []
        for task in self.tasks:
            result = task.execute(input_data)
            results.append(result)
        return results

    def __repr__(self):
        return f"Group({[str(t) for t in self.tasks]})"


class Pipeline:
    def __init__(self, *tasks: Task):
        self.tasks = tasks
        self._redis_client = None
        self._queue_name = None

    def execute(self, input_data: Any = None) -> Any:
        result = input_data
        for task in self.tasks:
            result = task.execute(result)
        return result

    def set_redis(self, redis_client: redis.Redis, queue_name: str):
        self._redis_client = redis_client
        self._queue_name = queue_name

    def to_dict(self) -> dict:
        return {
            "type": "pipeline",
            "tasks": [
                {"func": task.func.__name__, "args": task.args, "kwargs": task.kwargs}
                for task in self.tasks
            ]
        }

    def __repr__(self):
        return f"Pipeline({[str(t) for t in self.tasks]})"


class Worker:
    def __init__(self, redis_client: redis.Redis, worker_name: str = "worker"):
        self.redis = redis_client
        self.worker_name = worker_name
        self.task_queue = "task_queue"
        self.result_queue = "result_queue"

    def submit_task(self, task: Task) -> str:
        task_data = {
            "id": task.id,
            "func": task.func.__name__,
            "args": task.args,
            "kwargs": task.kwargs
        }
        self.redis.rpush(self.task_queue, json.dumps(task_data))
        return task.id

    def submit_group(self, group: Group, input_data: Any = None) -> str:
        group_data = {
            "type": "group",
            "tasks": [
                {"func": t.func.__name__, "args": t.args, "kwargs": t.kwargs}
                for t in group.tasks
            ],
            "input_data": input_data
        }
        group_id = f"group_{id(group)}"
        self.redis.rpush(self.task_queue, json.dumps({"id": group_id, **group_data}))
        return group_id

    def submit_pipeline(self, pipeline: Pipeline, input_data: Any = None) -> str:
        pipeline_data = pipeline.to_dict()
        pipeline_data["input_data"] = input_data
        pipeline_id = f"pipeline_{id(pipeline)}"
        self.redis.rpush(self.task_queue, json.dumps({"id": pipeline_id, **pipeline_data}))
        return pipeline_id

    def process_task(self, task_data: dict) -> Any:
        task_type = task_data.get("type", "task")
        
        if task_type == "group":
            tasks = []
            for t in task_data.get("tasks", []):
                func = globals().get(t["func"])
                if func:
                    tasks.append(Task(func, *t.get("args", []), **t.get("kwargs", {})))
            group = Group(*tasks)
            input_data = task_data.get("input_data")
            return group.execute_all(input_data)
        
        elif task_type == "pipeline":
            tasks = []
            for t in task_data.get("tasks", []):
                func = globals().get(t["func"])
                if func:
                    tasks.append(Task(func, *t.get("args", []), **t.get("kwargs", {})))
            pipeline = Pipeline(*tasks)
            input_data = task_data.get("input_data")
            return pipeline.execute(input_data)
        
        else:
            func_name = task_data.get("func")
            func = globals().get(func_name)
            if func:
                task = Task(func, *task_data.get("args", []), **task_data.get("kwargs", {}))
                return task.execute()
        return None

    def run(self):
        while True:
            result = self.redis.blpop(self.task_queue, timeout=5)
            if result:
                _, task_json = result
                task_data = json.loads(task_json)
                task_id = task_data.get("id")
                
                result_value = self.process_task(task_data)
                
                self.redis.hset(self.result_queue, task_id, json.dumps(result_value))


def add_one(x):
    return x + 1

def multiply_by_two(x):
    return x * 2

def square(x):
    return x * x


class LocalWorker:
    def __init__(self):
        pass

    def submit_pipeline(self, pipeline: Pipeline, input_data: Any = None) -> Any:
        return pipeline.execute(input_data)

    def submit_group(self, group: Group, input_data: Any = None) -> List[Any]:
        return group.execute_all(input_data)

    def submit_task(self, task: Task) -> Any:
        return task.execute()


if __name__ == "__main__":
    local_worker = LocalWorker()
    
    print("=== Pipeline 示例：先加1，再乘2 ===")
    pipeline = Pipeline(
        Task(add_one),
        Task(multiply_by_two)
    )
    result = local_worker.submit_pipeline(pipeline, 5)
    print(f"输入: 5")
    print(f"执行: add_one(5) -> 6, multiply_by_two(6) -> 12")
    print(f"输出: {result}")
    print()
    
    print("=== Group 示例：并行执行多个任务 ===")
    group = Group(
        Task(add_one),
        Task(multiply_by_two),
        Task(square)
    )
    results = local_worker.submit_group(group, 5)
    print(f"输入: 5")
    print(f"执行: add_one(5) = 6, multiply_by_two(5) = 10, square(5) = 25")
    print(f"输出: {results}")
