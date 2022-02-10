import flask
from flask import request, jsonify
from functions import *
import warnings
warnings.filterwarnings("ignore")


#Configure app
app = flask.Flask(__name__)
app.config["DEBUG"] = False

#Healthcheck Route
### We will use this route as a placeholder fro the / endpoint. It will tell if the service is online or not
@app.route('/', methods=['GET'])
def healthcheck():
    return jsonify({
        'status':'Online'
    })

#This route retrieves the execution of a given job
@app.route('/job/<job_id>', methods=['GET'])
def get_job(job_id):
    job_result=retrieve_job(job_id)

    if not job_result:
        #If job does not exist, warn the client
        return jsonify({
            'id': job_id,
            'status':'Not Created',
            'result':None,
            'created_at':None,
        })
    return jsonify({
        'id':job_id,
        'status':job_result[0][1],
        'result':job_result[0][2],
        'created_at':job_result[0][3]
    })

@app.route('/job', methods=['POST'])
def create():
    data=flask.request.json
    j_id = create_job()
    #Return the Job id to client
    response=jsonify(
        {
            'status':'Created!',
            'id':j_id
        }
    )
    #Flask will skip this function, write the return and come back to execute it
    @response.call_on_close
    def on_close():
        #Here we execute our function and log the results
        try:
            response_list=get_google_results(data['query'])
            status='Finished'
        except:
            # if anything happens during execution return fail
            response_list=[]
            status='Failed'
        update_job(j_id,status,{'content':response_list})
    return response,201




if __name__=='__main__':
    app.run(threaded=True)