#!/usr/bin/env python


"""Module bigjob_manager.

This Module is used to launch jobs via the advert service. 

It assumes that an bigjob_agent.py is available on the remote machine.
bigjob_agent.py will poll the advert service for new jobs and run these jobs on the respective
machine .

Background: This approach avoids queueing delays since the igjob_agent_launcher.py must be just started via saga.job or saga.cpr
once. All shortrunning task will be started using the protocol implemented by subjob() and bigjob_agent.py

Installation:
Set environment variable BIGJOB_HOME to installation directory
"""

import sys
from bigjob import logger
import time
import os
import traceback
import logging
import textwrap
import urlparse
import pdb

try:
    import paramiko
except:
    logger.warn("Paramiko not found. Without Paramiko file staging is not supported!")

from bigjob import SAGA_BLISS 
from bigjob.state import Running, New, Failed, Done, Unknown

if SAGA_BLISS == False:
    try:
        import saga
        logger.debug("Using SAGA C++/Python.")
        is_bliss=False
    except:
        logger.warn("SAGA C++ and Python bindings not found. Using Bliss.")
        try:
            import bliss.sagacompat as saga
            is_bliss=True
        except:
            logger.warn("SAGA Bliss not found")
else:
    logger.debug("Using SAGA Bliss.")
    try:
        import bliss.sagacompat as saga
        is_bliss=True 
    except:
        logger.warn("SAGA Bliss not found")

# import other BigJob packages
# import API
import api.base
sys.path.append(os.path.dirname(__file__))

from pbsssh import pbsssh
from sgessh import sgessh

if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( __file__ ) + "/ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 4):
    sys.path.append(os.path.dirname( __file__ ) + "/ext/subprocess-2.6.4/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 3):
    sys.stderr.write("Error: Python versions <2.3 not supported\n")
    sys.exit(-1)

import uuid

def get_uuid():
    wd_uuid=""
    wd_uuid += str(uuid.uuid1())
    return wd_uuid


""" Config parameters (will move to config file in future) """
CLEANUP=True

#for legacy purposes and support for old BJ API
pilot_url_dict={} # stores a mapping of pilot_url to bigjob



class BigJobError(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return repr(self.value)
    

class bigjob(api.base.bigjob):
    
    __APPLICATION_NAME="bigjob" 
    
    def __init__(self, coordination_url="advert://localhost/", pilot_url=None):    
        """ Initializes BigJob's coordination system
            e.g.:
            advert://localhost (SAGA/Advert SQLITE)
            advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
            redis://localhost:6379 (Redis at localhost)
            tcp://localhost (ZMQ)
        """  
        self.coordination_url = coordination_url
        self.coordination = self.__init_coordination(coordination_url)
        
        # restore existing BJ or initialize new BJ
        if pilot_url!=None:
            logger.debug("Reconnect to BJ: %s"%pilot_url)
            self.pilot_url=pilot_url
            self.uuid = self.__get_bj_id(pilot_url)
            self.app_url = self.__APPLICATION_NAME +":" + str(self.uuid)
            self.job = None
            self.working_directory = None
            self.state=self.get_state_detail()
            pilot_url_dict[self.pilot_url]=self
        else:
            self.uuid = "bj-" + str(get_uuid())        
            logger.debug("init BigJob w/: " + coordination_url)
            self.app_url =self. __APPLICATION_NAME +":" + str(self.uuid) 
            self.state=Unknown
            self.pilot_url=""
            self.job = None
            self.working_directory = None
            logger.debug("initialized BigJob: " + self.app_url)
        
       
    def __get_bj_id(self, pilot_url):
        start = pilot_url.index("bj-")
        end =pilot_url.index(":", start)
        return pilot_url[start:end]
    
     
    def __init_coordination(self, coordination_url):        
        if(coordination_url.startswith("advert://") or coordination_url.startswith("sqlasyncadvert://")):
            try:
                from coordination.bigjob_coordination_advert import bigjob_coordination
                logger.debug("Utilizing ADVERT Backend")
            except:
                logger.error("Advert Backend could not be loaded")
        elif (coordination_url.startswith("redis://")):
            try:
                from coordination.bigjob_coordination_redis import bigjob_coordination      
                logger.debug("Utilizing Redis Backend")
            except:
                logger.error("Error loading pyredis.")
        elif (coordination_url.startswith("tcp://")):
            try:
                from coordination.bigjob_coordination_zmq import bigjob_coordination
                logger.debug("Utilizing ZMQ Backend")
            except:
                logger.error("ZMQ Backend not found. Please install ZeroMQ (http://www.zeromq.org/intro:get-the-software) and " 
                      +"PYZMQ (http://zeromq.github.com/pyzmq/)")
        else:
            logger.error("No suitable coordination backend found.")
        
        logger.debug("Parsing URL: " + coordination_url)
        scheme, username, password, host, port, dbtype  = self.__parse_url(coordination_url) 
        
        if port == -1:
            port = None
        coordination = bigjob_coordination(server=host, server_port=port, username=username, 
                                           password=password, dbtype=dbtype, url_prefix=scheme)
        return coordination
    
   
            
    def start_pilot_job(self, 
                 lrms_url, 
                 bigjob_agent_executable=None,
                 number_nodes=1,
                 queue=None,
                 project=None,
                 working_directory=None,
                 userproxy=None,
                 walltime=None,
                 processes_per_node=1,
                 filetransfers=None):
        """ Start a batch job (using SAGA Job API) at resource manager. Currently, the following resource manager are supported:
            fork://localhost/ (Default Job Adaptor
            gram://qb1.loni.org/jobmanager-pbs (Globus Adaptor)
            pbspro://localhost (PBS Prop Adaptor)
        
        """
         
        if self.job != None:
            raise BigJobError("One BigJob already active. Please stop BigJob first.") 
            return

        ##############################################################################
        # initialization of coordination and communication subsystem
        # Communication & Coordination initialization
        lrms_saga_url = saga.url(lrms_url)
        self.pilot_url = self.app_url + ":" + lrms_saga_url.host
        pilot_url_dict[self.pilot_url]=self
        
        logger.debug("create pilot job entry on backend server: " + self.pilot_url)
        self.coordination.set_pilot_state(self.pilot_url, str(Unknown), False)
                
        logger.debug("set pilot state to: " + str(Unknown))
        ##############################################################################
        
        self.number_nodes=int(number_nodes)        
        
        # create job description
        jd = saga.job.description()
        
        # XXX Isn't the working directory about the remote site?
        # Yes, it is: This is to make sure that if fork
        if working_directory != None:
            if not os.path.isdir(working_directory) and lrms_saga_url.scheme=="fork":
                os.mkdir(working_directory)
            self.working_directory = working_directory
        else:
            # if no working dir is set assume use home directory
            # will fail if home directory is not the same on remote machine
            # but this is just a guess to avoid failing
            self.working_directory = os.path.expanduser("~") 
            
        # Stage BJ Input files
        # build target url
        # this will also create the remote directory for the BJ
        if lrms_saga_url.username!=None and lrms_saga_url.username!="":
            bigjob_working_directory_url = "ssh://" + lrms_saga_url.username + "@" + lrms_saga_url.host + self.__get_bigjob_working_dir()
        else:
            bigjob_working_directory_url = "ssh://" + lrms_saga_url.host + self.__get_bigjob_working_dir()
        
        # determine working directory of bigjob 
        # if a remote sandbox can be created via ssh => create a own dir for each bj job id
        # otherwise use specified working directory
        if self.__create_remote_directory(bigjob_working_directory_url)==True:
            self.working_directory = self.__get_bigjob_working_dir()
            self.__stage_files(filetransfers, bigjob_working_directory_url)
        else:        
            logger.warn("For file staging. SSH (incl. password-less authentication is required.")
         
        logger.debug("BJ Working Directory: %s", self.working_directory)
        logger.debug("Adaptor specific modifications: "  + str(lrms_saga_url.scheme))
        if lrms_saga_url.scheme == "condorg":
            jd.arguments = [ self.coordination.get_address(), self.pilot_url]
            agent_exe = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","bootstrap","bigjob-condor-bootstrap.py"))
            logger.debug("agent_exe",agent_exe)
            jd.executable = agent_exe             
        else:
            bootstrap_script = self.generate_bootstrap_script(self.coordination.get_address(), self.pilot_url)
            if lrms_saga_url.scheme == "gram":
                bootstrap_script = self.escape_rsl(bootstrap_script)
            elif lrms_saga_url.scheme == "pbspro" or lrms_saga_url.scheme=="xt5torque" or lrms_saga_url.scheme=="torque":                
                bootstrap_script = self.escape_pbs(bootstrap_script)
            elif lrms_saga_url.scheme == "ssh":
                bootstrap_script = self.escape_ssh(bootstrap_script)
            ############ submit pbs script which launches bigjob agent using ssh adaptors########## 
            elif lrms_saga_url.scheme == "pbs-ssh":
                bootstrap_script = self.escape_ssh(bootstrap_script)
                # PBS specific BJ plugin
                pbssshj = pbsssh(bootstrap_script, lrms_saga_url, walltime, number_nodes, 
                                 processes_per_node, userproxy, self.working_directory, self.working_directory)
                self.job = pbssshj
                self.job.run()
                return
            ############ submit sge script which launches bigjob agent using ssh adaptors########## 
            elif lrms_saga_url.scheme == "sge-ssh":
                bootstrap_script = self.escape_ssh(bootstrap_script)
                # PBS specific BJ plugin
                sgesshj = sgessh(bootstrap_script, lrms_saga_url, walltime, number_nodes, 
                                 processes_per_node, userproxy, project, queue, self.working_directory, self.working_directory)
                self.job = sgesshj
                self.job.run()
                return
            elif is_bliss:
                bootstrap_script = self.escape_bliss(bootstrap_script)

            #logger.debug(bootstrap_script)
            if is_bliss==False:
                jd.number_of_processes = str(number_nodes)
                jd.processes_per_host=str(processes_per_node)
            else:
                jd.TotalCPUCount=str(int(number_nodes)*int(processes_per_node))
                
            jd.spmd_variation = "single"
            #jd.arguments = [bigjob_agent_executable, self.coordination.get_address(), self.pilot_url]
            jd.arguments = ["python", "-c", bootstrap_script]
            jd.executable = "/usr/bin/env"
            if queue != None:
                jd.queue = queue
            if project !=None:
                jd.job_project = [project]
            if walltime!=None:
                jd.wall_time_limit=str(walltime)
        
            jd.working_directory = self.working_directory
    
            logger.debug("Working directory: " + jd.working_directory)
            jd.output = os.path.join(self.working_directory, "stdout-bigjob_agent.txt")
            jd.error = os.path.join(self.working_directory,"stderr-bigjob_agent.txt")
          
           
        # Submit job
        js = None    
        if userproxy != None and userproxy != '':
            s = saga.session()
            os.environ["X509_USER_PROXY"]=userproxy
            ctx = saga.context("x509")
            ctx.set_attribute ("UserProxy", userproxy)
            s.add_context(ctx)
            logger.debug("use proxy: " + userproxy)
            js = saga.job.service(s, lrms_saga_url)
        else:
            logger.debug("use standard proxy")
            js = saga.job.service(lrms_saga_url)

        logger.debug("Creating pilot job with description: %s" % str(jd))
              

        self.job = js.create_job(jd)
        logger.debug("Submit pilot job to: " + str(lrms_saga_url))
        self.job.run()
        return self.pilot_url
        
    def generate_bootstrap_script(self, coordination_host, coordination_namespace):
        script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
start_time = time.time()
home = os.environ.get("HOME")
BIGJOB_AGENT_DIR= os.path.join(home, ".bigjob")
if not os.path.exists(BIGJOB_AGENT_DIR): os.mkdir (BIGJOB_AGENT_DIR)
BIGJOB_PYTHON_DIR=BIGJOB_AGENT_DIR+"/python/"
BOOTSTRAP_URL="https://raw.github.com/saga-project/BigJob/master/bootstrap/bigjob-bootstrap.py"
BOOTSTRAP_FILE=BIGJOB_AGENT_DIR+"/bigjob-bootstrap.py"
#ensure that BJ in .bigjob is upfront in sys.path
sys.path.insert(0, os.getcwd() + "/../")
sys.path.insert(0, os.getcwd() + "/../../")
p = list()
for i in sys.path:
    if i.find(\".bigjob/python\")>1:
          p.insert(0, i)
for i in p: sys.path.insert(0, i)
print str(sys.path)
try: import saga
except: print "SAGA and SAGA Python Bindings not found: BigJob only work w/ non-SAGA backends e.g. Redis, ZMQ.";print "Python version: ",  os.system("python -V");print "Python path: " + str(sys.path)
try: import bigjob.bigjob_agent
except: print "BigJob not installed. Attempting to install it."; opener = urllib.FancyURLopener({}); opener.retrieve(BOOTSTRAP_URL, BOOTSTRAP_FILE); os.system("python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR); activate_this = BIGJOB_PYTHON_DIR+'bin/activate_this.py'; execfile(activate_this, dict(__file__=activate_this))
#try to import BJ once again
import bigjob.bigjob_agent
# execute bj agent
args = list()
args.append("bigjob_agent.py")
args.append(\"%s\")
args.append(\"%s\")
print "Bootstrap time: " + str(time.time()-start_time)
print "Starting BigJob Agents with following args: " + str(args)
bigjob_agent = bigjob.bigjob_agent.bigjob_agent(args)
""" % (coordination_host, coordination_namespace))
        return script
    
    def escape_rsl(self, bootstrap_script):
        logger.debug("Escape RSL")
        bootstrap_script = bootstrap_script.replace("\"", "\"\"")
        return bootstrap_script
    
    
    def escape_pbs(self, bootstrap_script):
        logger.debug("Escape PBS")
        bootstrap_script = "\'" + bootstrap_script+ "\'"
        return bootstrap_script
    
    
    def escape_ssh(self, bootstrap_script):
        logger.debug("Escape SSH")
        bootstrap_script = bootstrap_script.replace("\"", "\\\"")
        bootstrap_script = bootstrap_script.replace("\'", "\\\"")
        bootstrap_script = "\"" + bootstrap_script+ "\""
        return bootstrap_script
    
    def escape_bliss(self, bootstrap_script):
        logger.debug("Escape fork")
        bootstrap_script = bootstrap_script.replace("\'", "\"")
        bootstrap_script = "\'" + bootstrap_script+ "\'"
        return bootstrap_script
     
    def list_subjobs(self):
        sj_list = self.coordination.get_jobs_of_pilot(self.pilot_url)
        logger.debug(str(sj_list))
        subjobs = []
        for i in sj_list:
            url = i 
            if url.find("/")>0:
                url = url[url.find("bigjob"):]
                url =  url.replace("/", ":")    
            sj = subjob(job_url=url)
            subjobs.append(sj)
        return subjobs
     
    def add_subjob(self, jd, job_url, job_id):
        logger.debug("Stage input files for sub-job")
        if jd.attribute_exists ("filetransfer"):
            try:
                self.__stage_files(jd.filetransfer, self.__get_subjob_working_dir(job_id))
            except:
                logger.error("File Stagein failed. Is Paramiko installed?")
        logger.debug("add subjob to queue of PJ: " + str(self.pilot_url))        
        for i in range(0,3):
            try:
                logger.debug("create dictionary for job description. Job-URL: " + job_url)
                # put job description attributes to Redis
                job_dict = {}
                #to accomendate current bug in bliss (Number of processes is not returned from list attributes)
                job_dict["NumberOfProcesses"] = "1" 
                attributes = jd.list_attributes()   
                logger.debug("SJ Attributes: " + str(jd))             
                for i in attributes:          
                        if jd.attribute_is_vector(i):
                            #logger.debug("Add attribute: " + str(i) + " Value: " + str(jd.get_vector_attribute(i)))
                            vector_attr = []
                            for j in jd.get_vector_attribute(i):
                                vector_attr.append(j)
                            job_dict[i]=vector_attr
                        else:
                            #logger.debug("Add attribute: " + str(i) + " Value: " + jd.get_attribute(i))
                            job_dict[i] = jd.get_attribute(i)
                
                job_dict["state"] = str(Unknown)
                job_dict["job-id"] = str(job_id)
                
                #logger.debug("update job description at communication & coordination sub-system")
                self.coordination.set_job(job_url, job_dict)                                                
                self.coordination.queue_job(self.pilot_url, job_url)
                break
            except:
                traceback.print_exc(file=sys.stdout)
                time.sleep(2)
                #raise Exception("Unable to submit job")
                     
    def delete_subjob(self, job_url):
        self.coordination.delete_job(job_url) 
    
    def set_attribute(self, job_url,sj):
        self.coordination.set_job(job_url,sj) 

    def get_subjob_state(self, job_url):
        return self.coordination.get_job_state(job_url) 
    
    def get_subjob_details(self, job_url):
        return self.coordination.get_job(job_url) 
     
    def get_state(self):        
        """ duck typing for get_state of saga.job.job  
            state of saga job that is used to spawn the pilot agent
        """
        try:
            return self.job.get_state()
        except:
            return self.get_state_detail()
            #return None
    
    def get_state_detail(self): 
        """ internal state of BigJob agent """ 
        try:
            return self.coordination.get_pilot_state(self.pilot_url)["state"]
        except:
            return None
    
    def get_free_nodes(self):
        jobs = self.coordination.get_jobs_of_pilot(self.pilot_url)
        number_used_nodes=0
        for i in jobs:
            job_detail = self.coordination.get_job(i)            
            if job_detail != None and job_detail.has_key("state") == True\
                and job_detail["state"]==str(Running):
                job_np = "1"
                if (job_detail["NumberOfProcesses"] == True):
                    job_np = job_detail["NumberOfProcesses"]
                number_used_nodes=number_used_nodes + int(job_np)
        return (self.number_nodes - number_used_nodes)

    
    def stop_pilot_job(self):
        """ mark in advert directory of pilot-job as stopped """
        try:
            logger.debug("stop pilot job: " + self.pilot_url)
            self.coordination.set_pilot_state(self.pilot_url, str(Done), True)            
            self.job=None
        except:
            pass
    
    def cancel(self):        
        """ duck typing for cancel of saga.cpr.job and saga.job.job  """
        logger.debug("Cancel Pilot Job")
        try:
            self.job.cancel()
        except:
            pass
            #traceback.print_stack()
        try:            
            self.stop_pilot_job()
            logger.debug("delete pilot job: " + str(self.pilot_url))                      
            if CLEANUP:
                self.coordination.delete_pilot(self.pilot_url)                
        except:
            pass
            #traceback.print_stack()

    def wait(self):
        """ Waits for completion of all sub-jobs """        
        while 1:
            jobs = self.coordination.get_jobs_of_pilot(self.pilot_url)
            finish_counter=0
            result_map = {}
            for i in jobs:
                state = self.coordination.get_job_state(str(i))            
                #state = job_detail["state"]                
                if result_map.has_key(state)==False:
                    result_map[state]=1
                else:
                    result_map[state] = result_map[state]+1
                if self.__has_finished(state)==True:
                    finish_counter = finish_counter + 1                   
            logger.debug("Total Jobs: %s States: %s"%(len(jobs), str(result_map)))
            if finish_counter == len(jobs):
                break
            time.sleep(2)


    ###########################################################################
    # internal methods
    
    def __has_finished(self, state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False
    
    def __parse_url(self, url):
        try:
            if is_bliss==True:
                raise BigJobError("BLISS URL broken.")
            surl = saga.url(url)
            host = surl.host
            port = surl.port
            username = surl.username
            password = surl.password
            query = surl.query
            scheme = "%s://"%surl.scheme
        except:
            """ Fallback URL parser based on Python urlparse library """
            logger.error("URL %s could not be parsed"%(url))
            traceback.print_exc(file=sys.stderr)
            result = urlparse.urlparse(url)
            logger.debug("Result: " + str(result))
            host = result.hostname
            #host = None
            port = result.port
            username = result.username
            password = result.password
            scheme = "%s://"%result.scheme 
            if host==None:
                logger.debug("Python 2.6 fallback")
                if url.find("/", len(scheme)) > 0:
                    host = url[len(scheme):url.find("/", len(scheme))]
                else:
                    host = url[len(scheme):]
                if host.find(":")>1:
                    logger.debug(host)
                    comp = host.split(":")
                    host = comp[0]
                    port = int(comp[1])
                    
            if url.find("?")>0:
                query = url[url.find("?")+1:]
            else:
                query = None
            
        
        logger.debug("%s %s %s"%(scheme, host, port))
        return scheme, username, password, host, port, query     
            
    
    def __get_bigjob_working_dir(self):
        return os.path.join(self.working_directory, self.uuid)
    
    
    def __get_subjob_working_dir(self, sj_id):
        return os.path.join(self.__get_bigjob_working_dir(), sj_id)
    

    def __stage_files(self, filetransfers, target_url):
        logger.debug("Stage: %s to %s"%(filetransfers, target_url))
        if filetransfers==None:
            return
        for i in filetransfers:
            source_file=i
            if i.find(">")>0:
                source_file = i[:i.find(">")].strip()
            target_url_full = os.path.join(target_url, os.path.basename(source_file))
            logger.debug("Stage: %s to %s"%(source_file, target_url_full))
            self.__third_party_transfer(source_file, target_url_full)
           
        
    def __third_party_transfer(self, source_url, target_url):
        """
            Transfers from source URL to machine of PS (target path)
        """
        result = urlparse.urlparse(source_url)
        source_host = result.netloc
        source_path = result.path
        
        result = urlparse.urlparse(target_url)
        target_host = result.netloc
        target_path = result.path
          
        python_script= """import sys
import os
import urllib
import sys
import time
import paramiko

client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("%s")
sftp = client.open_sftp()
sftp.put("%s", "%s")
"""%(target_host, source_path, target_path)

        logging.debug("Execute: \n%s"%python_script)
        source_client = paramiko.SSHClient()
        source_client.load_system_host_keys()
        source_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        source_client.connect(source_host)
        stdin, stdout, stderr = source_client.exec_command("python -c \'%s\'"%python_script)
        stdin.close()
        logging.debug("************************************************")
        logging.debug("Stdout: %s\nStderr:%s", stdout.read(), stderr.read())
        logging.debug("************************************************")
      
    
    def __create_remote_directory(self, target_url):
        #result = urlparse.urlparse(target_url)
        #target_host = result.netloc
        #target_path = result.path
        
        # Python 2.6 compatible URL parsing
        scheme = target_url[:target_url.find("://")+3]
        target_host = target_url[len(scheme):target_url.find("/", len(scheme))]
        target_path = target_url[len(scheme)+len(target_host):]    
        target_user = None
        if target_host.find("@")>1:
            comp = target_host.split("@")
            target_host =comp[1]
            target_user =comp[0]
        logger.debug("Create remote directory; scheme: %s, host: %s, path: %s"%(scheme, target_host, target_path))
        if scheme.startswith("fork") or target_host.startswith("localhost"):
            os.makedirs(target_path)
            return True
        else:
            try:
                client = self.__get_ssh_client(target_host, target_user)
                sftp = client.open_sftp()            
                sftp.mkdir(target_path)
                sftp.close()
                client.close()
                return True
            except:
                self.__print_traceback()	
                logger.warn("Error creating directory: " + str(target_path) 
                             + " at: " + str(target_host) + " SSH password-less login activated?" )
                return False
             
        
    def __get_ssh_client(self, hostname, user=None):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if user == None: user = self.__discover_ssh_user(hostname)
        if user!=None: logger.debug("discovered user: " + user)
        client.connect(hostname, username=user)
        return client
    
    
    def __discover_ssh_user(self, hostname):
        # discover username
        user = None
        ssh_config = os.path.join(os.path.expanduser("~"), ".ssh/config")
        ssh_config_file = open(ssh_config, "r")
        lines = ssh_config_file.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            if line.find(hostname)>0:
                for k in range(i + 1, len(lines)):
                    sub_line = lines[k]
                    if sub_line.startswith(" ")==True and sub_line.startswith("\t")==True:
                        break # configuration for next host
                    elif sub_line.find("User")!=-1:
                        stripped_sub_line = sub_line.strip()
                        user = stripped_sub_line.split()[1]
                        break
        ssh_config_file.close() 
        return user
    
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
        
    def __repr__(self):
        return self.pilot_url 

    def __del__(self):
        """ BJ is not cancelled when object terminates
            Application can reconnect to BJ via pilot url later on"""
        pass
        #self.cancel()


                    
                    
class subjob(api.base.subjob):
    
    def __init__(self, coordination_url=None, job_url=None):
        """Constructor"""
        
        self.coordination_url = coordination_url
        if job_url!=None:
            self.job_url=job_url
            self.uuid = self.__get_sj_id(job_url)
            self.pilot_url = self.__get_pilot_url(job_url)
            logger.debug("Reconnect SJ: %s Pilot %s"%(self.job_url, self.pilot_url))
        else:
            self.uuid = "sj-" + str(get_uuid())
            self.job_url = None
            self.pilot_url = None
            self.bj = None
      
    def set_attribute(self, attribute,value,pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        sj = self.bj.get_subjob_details(self.job_url)
        sj[attribute]=value
        self.bj.set_attribute(self.job_url,sj)

    def get_attribute(self, attribute,pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        sj = self.bj.get_subjob_details(self.job_url)
        return sj[attribute]
            
    
    def __get_sj_id(self, job_url):
        start = job_url.index("sj-")        
        return job_url[start:]
    
    
    def __get_pilot_url(self, job_url):
        end =job_url.index(":jobs")
        return job_url[:end]    
    
                    
    def get_job_url(self, pilot_url):
        self.job_url = pilot_url + ":jobs:" + str(self.uuid)
        return self.job_url
    

    def submit_job(self, pilot_url, jd):
        """ submit subjob to referenced bigjob """
        if self.job_url==None:
            self.job_url=self.get_job_url(pilot_url)            
        
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]    
        self.bj.add_subjob(jd, self.job_url, self.uuid)


    def get_state(self, pilot_url=None):        
        """ duck typing for saga.job  """
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]                
        return self.bj.get_subjob_state(self.job_url)
    
    def cancel(self, pilot_url=None):
        logger.debug("delete job: " + self.job_url)
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        if str(self.bj.get_state())=="Running":
            self.bj.delete_subjob(self.job_url)        
        
    def get_exe(self, pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        sj = self.bj.get_subjob_details(self.job_url)
        return sj["Executable"]
   
    def get_arguments(self, pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        sj = self.bj.get_subjob_details(self.job_url)  
        #logger.debug("Subjob details: " + str(sj))              
        arguments=""
        for  i in  sj["Arguments"]:
            arguments = arguments + " " + i
        return arguments

    def __del__(self):
        """ Do nothing - keep subjobs alive and allow a later reconnection"""
        pass
        #self.cancel()
    
    def __repr__(self):        
        if(self.job_url==None):
            return "None"
        else:
            return self.job_url
        
        
class description(saga.job.description):
    pass
