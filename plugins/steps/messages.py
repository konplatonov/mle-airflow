from airflow.providers.telegram.hooks.telegram import TelegramHook

# Конфигурация
TELEGRAM_TOKEN = '7062461326:AAHHPzNbMkm0DiTI2icy1CrMm4DjxI0qs7E'
CHAT_ID = '-4910773410'

# Отправка при ошибке
def send_telegram_failure_message(context):
    hook = TelegramHook(token=TELEGRAM_TOKEN, chat_id=CHAT_ID)
    dag = context['dag'].dag_id
    run_id = context['run_id']
    task_key = context.get('task_instance_key_str')

    message = (
        f"❌ *Ошибка выполнения DAG!*\n\n"
        f"*DAG:* `{dag}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"*Task:* `{task_key}`"
    )

    hook.send_message({
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    })

# Отправка при успехе
def send_telegram_success_message(context):
    hook = TelegramHook(token=TELEGRAM_TOKEN, chat_id=CHAT_ID)
    dag = context['dag'].dag_id
    run_id = context['run_id']

    message = (
        f"✅ *DAG успешно завершён!*\n\n"
        f"*DAG:* `{dag}`\n"
        f"*Run ID:* `{run_id}`"
    )

    hook.send_message({
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    })
