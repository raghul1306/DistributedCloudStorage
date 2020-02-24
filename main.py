import os
from os import path
import urllib.request
from flask import Flask, request, redirect, jsonify,send_from_directory,make_response,send_file
import uuid
import json
from werkzeug.utils import secure_filename
import math
import hashlib

UPLOAD_FOLDER = './uploads'
TEMP_FOLDER = './temp'


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['TEMP_FOLDER'] = TEMP_FOLDER


file_id_map = {}
block_map={}
files_in_node={}
hashes={}

with open('config.json') as f:
  config = json.load(f)

for i in range(config['node_count']):
    directory = "node_"+str(i+1)
    dpath = os.path.join(UPLOAD_FOLDER, directory)
    if not path.exists(dpath):
        os.mkdir(dpath)
    files_in_node[i+1]=0

@app.route('/files', methods=['PUT'])
def upload_file():
    file = request.files['file']
    if(secure_filename(file.filename)) in file_id_map.values():
        return "File Already Exists",409

    
    filename = secure_filename(file.filename)
    file_uuid = str(uuid.uuid1())
    file_id_map[file_uuid]=filename
    block_map[file_uuid]={}
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    size = os.stat(os.path.join(app.config['UPLOAD_FOLDER'], filename)).st_size
    
    partnum = 0
    nodenum=0
    input = open(os.path.join(app.config['UPLOAD_FOLDER'], filename), 'rb')
    
    while 1:
        chunk = input.read(config['size_per_slice'])
        if not chunk: break
        temp = '-part{0:07d}'.format(partnum)
        fname='{}{}'.format(secure_filename(file.filename),temp)
        nodefolders=[]
        partnum  = partnum+1
        

        for _ in range(config['redundancy_count']+1):
            nodenum = min(files_in_node.keys(), key=(lambda k: files_in_node[k]))
            # nodenum = (nodenum%10)+1
            nodefolder='node_'+str(nodenum)
            nodefolders.append(nodefolder)
            chunk_loc = os.path.join(UPLOAD_FOLDER,nodefolder) 
            files_in_node[nodenum] += 1
            
            filename = os.path.join(chunk_loc, fname)
            fileobj  = open(filename, 'wb')
            fileobj.write(chunk)
            fileobj.close() 
            file_hash=hashlib.md5()
            with open(filename, 'rb') as f:
                fb = f.read(1024) 
                while len(fb) > 0: 
                    file_hash.update(fb) 
                    fb = f.read(1024)
        hashes[fname] = file_hash.hexdigest()
        block_map[file_uuid][fname] = nodefolders
    input.close()
    with open('file_id_map.json', 'w') as fp:
        json.dump(file_id_map,fp)
    with open('block_map.json', 'w') as fp:
        json.dump(block_map,fp)
    with open('files_in_nodes.json','w') as fp:
        json.dump(files_in_node,fp)

    os.remove(os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename)))
    return str(file_uuid)
    
    
@app.route('/files/list')
def list():
    files = []
    for i in file_id_map:
        files.append({"file_name":file_id_map[i],"id":i})
    
    return jsonify(files)

@app.route("/files/<id>")
def get_file(id):
    """Download a file."""
    if id in file_id_map:
        output = open(os.path.join(app.config['TEMP_FOLDER'], file_id_map[id]), 'wb')
        parts=[]
        for blockname in block_map[id]:
            parts.append(blockname)
        parts.sort()
        
        for filename in parts:
            found=False
            modified=0
            for loc in block_map[id][filename]:
                if(path.exists(os.path.join(app.config['UPLOAD_FOLDER'],loc))):
                    node = loc
                    found=True
                    filepath=os.path.join(app.config['UPLOAD_FOLDER'], node)
                    filepath = os.path.join(filepath, filename)
                    file_hash=hashlib.md5()
                    with open(filepath, 'rb') as f:
                        fb = f.read(1024) 
                        while len(fb) > 0: 
                            file_hash.update(fb) 
                            fb = f.read(1024)
                    if(hashes[filename] != file_hash.hexdigest()):
                        print(filepath,'not equal')
                        modified +=1
                        continue
                    fileobj  = open(filepath, 'rb')
                    # print("Reading from ",filepath)
                    while 1:
                        filebytes = fileobj.read(config['size_per_slice'])
                        if not filebytes: break
                        output.write(filebytes)
                    fileobj.close(  )
                    
                    break
            if (not found):
                return "Files Missing",400
            if modified>config['redundancy_count']:
                return "File Modified",500
        output.close(  )
        # response = make_response(send_from_directory(TEMP_FOLDER, file_id_map[id], as_attachment=True))
        # response.headers['Content-Type'] = 'application/pdf'
        # response = send_file(os.path.join(app.config['TEMP_FOLDER'], file_id_map[id]))
        # response.headers['Content-Type'] = 'application/pdf'
        # return response
        return send_file(os.path.join(app.config['TEMP_FOLDER'], file_id_map[id]))
        # return send_from_directory(TEMP_FOLDER, file_id_map[id], as_attachment=True)
    else:
        resp = "requested object "+id+" is not found"
        return resp,404

@app.route("/files/<id>",methods=['DELETE'])
def deletefile(id):
    if id in file_id_map:
        parts=[]
        for blockname in block_map[id]:
            parts.append(blockname)
        parts.sort()
        for filename in parts:
            for folder in block_map[id][filename]:
                filepath=os.path.join(app.config['UPLOAD_FOLDER'], folder)
                filepath = os.path.join(filepath, filename)
                os.remove(filepath)
        resp = "object "+id+" deleted successfully"
        del file_id_map[id]
        return resp
    else:
        resp = "requested object "+id+" is not found"
        return resp,404
if __name__ == "__main__":
    app.run(debug=True)