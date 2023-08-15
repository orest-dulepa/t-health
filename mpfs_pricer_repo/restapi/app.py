import json
import os
from pybrake.flask import init_app
from flask import Flask, request, url_for, jsonify, Response
from worker.tasks import price_claim_data

app = Flask(__name__)

airbrake_project_id = os.environ.get('AIRBRAKE_PROJECT_ID', '')
airbrake_project_key = os.environ.get('AIRBRAKE_PROJECT_KEY', '')
if airbrake_project_id != '' and airbrake_project_key != '':
    app.config["PYBRAKE"] = dict(
        project_id=airbrake_project_id,
        project_key=airbrake_project_key,
        environment="production"
    )
    app = init_app(app)


@app.route('/price_claim/', methods=['POST'])
def price_claim():
    data = request.json
    if data is None:
        return Response(
            "No claims submitted. Please submit the claims to price as an application/json payload. ",
            status=400,
        )

    immediately = json.loads(request.args.get('immediately', 'false'))
    if immediately:
        return jsonify({"result": price_claim_data(data)}), 200
    task = price_claim_data.delay(data)
    return jsonify({
        "result_url": url_for('price_claim_result', task_id=task.id),
    }), 202, {'Location': url_for('price_claim_result', task_id=task.id)}


@app.route('/price_claim/<task_id>', methods=['GET'])
def price_claim_result(task_id):
    task = price_claim_data.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'pricing': None,
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'pricing': task.info,
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'pricing': None,
            'error_message': str(task.info),
        }
    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)
