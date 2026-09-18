from callbacks.agent_failure_callback import notify_dag_failure_agent

def policy(task):
    if not task.on_failure_callback:
        task.on_failure_callback = notify_dag_failure_agent