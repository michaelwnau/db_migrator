from flask import Flask, request, jsonify
from celery import Celery
from flask_graphql import GraphQLView
from schema import schema
from your_migration_module import migrate_database

app = Flask(__name__)

# Configure Celery
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Add this line to register the GraphQL endpoint
app.add_url_rule('/graphql', view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))

@celery.task
def migrate_db_task(source_connection_params, target_connection_params, export_file_path, s3_bucket, s3_key, aws_access_key, aws_secret_key):
    migrate_database(source_connection_params, target_connection_params,
                     export_file_path, s3_bucket, s3_key, aws_access_key, aws_secret_key)


@app.route('/migrate', methods=['POST'])
def migrate_database_endpoint():
    data = request.get_json()
    source_connection_params = data['source_connection_params']
    target_connection_params = data['target_connection_params']
    export_file_path = data['export_file_path']
    s3_bucket = data['s3_bucket']
    s3_key = data['s3_key']
    aws_access_key = data['aws_access_key']
    aws_secret_key = data['aws_secret_key']

    task = migrate_db_task.delay(source_connection_params, target_connection_params,
                                 export_file_path, s3_bucket, s3_key, aws_access_key, aws_secret_key)

    return jsonify({"message": "Migration started", "task_id": task.id}), 202


@app.route('/migrate/<task_id>', methods=['GET'])
def check_migration_status(task_id):
    task = migrate_db_task.AsyncResult(task_id)
    return jsonify({"status": task.status, "result": task.result})


if __name__ == '__main__':
    app.run(debug=True)
