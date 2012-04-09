#############################################
#  Centralized Synchronous RE-Exchange       
#############################################

import sys
import os
import random
import time
import optparse
import logging
import re
import math
import threading
import traceback
import pdb
import ConfigParser
import saga
import bigjob

from bigjob import bigjob, subjob, description


class ReManager():

   ############################################################################
   #  This class hold information about the application and replicas runnning 
   ###########################################################################
   def __init__(self, config_filename):

       self.exchange_count = 0
       self.arguments = []

       self.replica_count = 0
       self.temperatures = []

       self.replica_jobs = []

       self.read_config(config_filename)
       random.seed(time.time()/10.)

   def read_config(self,conf_file):

       # read config file
       config = ConfigParser.ConfigParser()
       print ("read configfile: " + conf_file)
       config.read(conf_file)

       # RE Configuration
       default_dict = config.defaults()
       print "\n (INFO)" + " reading values "
       #self.arguments = default.dict["arguments"].split()
       self.total_number_replica = config.getint("DEFAULT", "total_replica_count")
       print "\n replica count is " + str(self.total_number_replica)
       self.number_of_nodes = config.getint("DEFAULT", "number_of_nodes")
       print "\n number of nodes is " + str(self.number_of_nodes)
       self.exchange_count = config.getint("DEFAULT", "exchange_count")
       print "\n exchange count is " + str(self.exchange_count)
       self.working_directory = default_dict["working_directory"]
       print "\n working directory path is: " + self.working_directory
       self.replica_directory= default_dict["replica_directory"]
       print "\n replica_directory path is: " + self.replica_directory
       self.host= default_dict["host"]
       print "\n Host of the application is: " + self.host
       self.cores_per_replica = default_dict["cores_per_replica"]       
       print "\n number of cores per replica is: " + self.cores_per_replica
       self.temperatures = default_dict["temperature"].split()
       self.COORDINATION_URL = default_dict["coordination_url"] 
   
   def start_bigjob(self,COORDINATION_URL,RESOURCEMGR_URL):

       ##########################################################################################
       # make sure you are familiar with the queue structure on futuregrid,ppn, your project id
       # and the walltime limits on each queue. change accordingly
       #
       queue="normal"          # Queue to which BigJob has to be submitted, if None, default queue is considered.
       project=None            # Allocation Information. if None, default information is considered
       walltime=60             # Time in minutes. There are limits on the time you can request

       processes_per_node=8    # ppn
       number_of_processes=64  # The total number of processes ( BigJob size), used to run Jobs
       workingdirectory= os.path.join(os.getcwd(), "agent") # working directory for agent.
       ##########################################################################################
       
       print "\n (INFO) Start Pilot Job/BigJob at: " + RESOURCEMGR_URL
       start= time.time()
       self.bj = bigjob(COORDINATION_URL)
       self.bj.start_pilot_job( RESOURCEMGR_URL,
                                None,
                                number_of_processes,
                                queue,
                                project,
                                workingdirectory,
                                None,
                                walltime,
                                processes_per_node)
         
       print "\n (INFO) Pilot Job/BigJob URL: " + self.bj.pilot_url + " State: " + str(self.bj.get_state())
       logging.debug("BigJob Initiation time: " + str(time.time()-start))
       print "\n (INFO) BigJob Initiation time: " + str(time.time()-start)
       return self.bj
   

   def stage_in_files(self,replica_id):
       #start = time.time()
       #print "\n (INFO) Staging Files "
       try:
          os.mkdir(self.working_directory + 'agent/' + str(replica_id))
       except OSError:
          pass 
       #  print "Unexpected error:", sys.exc_info() [0]
       os.system("cp -r " + self.replica_directory + "* " + self.working_directory + "agent/" + str(replica_id) + "/")
       #print "\n (INFO) Staging Files Done "
       #print "\n (INFO) total time taken to stage files is: " + str(time.time()-start)

 
   def prepare_NAMD_config(self, replica_id):
       # The idea behind this is that we can simply modify NPT.conf before submit a job to set temp and other variables
       ifile = open(self.working_directory +"NPT.conf")   # should be changed if a different name is going to be used
       lines = ifile.readlines()
       for line in lines:
           if line.find("desired_temp") >= 0 and line.find("set") >= 0:
               items = line.split()
               temp = items[2]
               if eval(temp) != self.temperatures[replica_id]:
                   print "\n (DEBUG) temperature is changing to " + str(self.temperatures[replica_id]) + " from " + temp + " for rep" + str(replica_id)
                   lines[lines.index(line)] = "set desired_temp %s \n"%(str(self.temperatures[replica_id]))
       ifile.close() 
       ofile = open(self.working_directory + "NPT.conf","w")
       for line in lines:    
           ofile.write(line)
       ofile.close()
   
   def transfer_NPT(self, replica_id):
       start =  time.time()
       try:
          os.system("cp " + self.working_directory + "NPT.conf " + self.working_directory + "agent/" + str(replica_id) + "/NPT.conf")
       except OSError:
          print "Unexpected error:", sys.exc_info() [0]

   def get_job_description(self, replica_id):        

       jd = description()  
       jd.executable = self.working_directory + "agent/" + str(replica_id) + "/namd2"
       jd.number_of_processes = "8" 
       jd.spmd_variation = "single"
       jd.arguments = ["NPT.conf"] 
       jd.working_directory = self.working_directory + "agent/" + str(replica_id) + "/"
       jd.output = "stdout-" + str(replica_id) + ".txt"
       jd.error = "stderr-" + str(replica_id) + ".txt"
       
       return jd

   def submit_subjob(self, jd):
       #######  submit job via pilot job ######
       sj = subjob()
       sj.submit_job(self.bj.pilot_url, jd)
       self.job_start_times[sj]=time.time()
       self.job_states[sj] = sj.get_state()
 
       return sj

   def get_energy(self, replica_id):
        """ parse energy out of stdout """
        #stdout = self.replica_jobs[replica_id].get_stdout()
        stdoutfile = open(self.working_directory + "agent/"+ str(replica_id) + "/stdout-" + str(replica_id) + ".txt")  
        stdout = stdoutfile.readlines()
        for line in stdout:
            items = line.split()
            if len(items) > 0:
                if items[0] in ("ENERGY:"):
                    en = items[11]  
        print "(DEBUG) energy : " + str(en) + " from replica " + str(replica_id) 
        return eval(en) 
 
   def do_exchange(self, energy, irep, jrep):
        iflag = False
        en_a = energy[irep]
        en_b = energy[jrep]
        
        factor = 0.0019872  # from R = 1.9872 cal/mol
        delta = (1./int(self.temperatures[irep])/factor - 1./int(self.temperatures[irep+1])/factor)*(en_b-en_a)
        if delta < 0:
            iflag = True
        else :
            if math.exp(-delta) > random.random() :
                iflag = True
    
        if iflag is True:
            tmpNum = self.temperatures[jrep]
            self.temperatures[jrep] = self.temperatures[irep]
            self.temperatures[irep] = tmpNum
    
        print "(DEBUG) delta = %f"%delta + " en_a = %f"%en_a + " from rep " + str(irep) + " en_b = %f"%en_b +" from rep " + str(jrep)
   
   def stop_bigjob(self):

       ##################
       #  Stop Pilot Job
       ##################
       self.bj.cancel() 
       
 
   ############################################################################
   # run_REMDg
   ############################################################################
   def run_REMDg(self):
        
       ###### Main loop which runs the replica-exchange  ####
       start = time.time()
       COORDINATION_URL= self.COORDINATION_URL
       RESOURCEMGR_URL= self.host
       numEX = self.exchange_count
       ofilename = "remd-temp.out"

       print "\n (INFO) Start BigJob"
       self.bj= self.start_bigjob(COORDINATION_URL,RESOURCEMGR_URL)
       if self.bj==None or self.bj.get_state()=="Failed":
            return


       iEX = 0
       total_number_of_namd_jobs = 0
       while 1 :
            print "\n"
            # reset replica number

            print "#################### spawn jobs ####################"
            self.replica_jobs = []
            self.job_states= {}
            self.job_start_times = {}
            start_time = time.time()
            replica_id = 0
            state = self.bj.get_state()                        
            print "\n (INFO) BigJob State: " + str(state)
            pilot_url = self.bj.pilot_url
            print " Pilot: " + pilot_url + "state: " + str(state)
            
            if str(state) == "Running":
               logging.debug("pilot job running: " + str(self.total_number_replica) + "jobs.")
               for i in range (0, self.total_number_replica):         
               ############## replica job spawn ############
                   start=time.time()
                   self.stage_in_files(replica_id)
                   print "\n (INFO) total time taken to stage files is: " + str(time.time()-start)
                   self.prepare_NAMD_config(replica_id)
                   self.transfer_NPT(replica_id)
                   jd = self.get_job_description(replica_id)
                   new_job = self.submit_subjob(jd)
                   self.replica_jobs.insert(replica_id, new_job)
                   replica_id = replica_id + 1
                   print "(INFO) Replica " + "%d"%replica_id + " started (Num of Exchange Done = %d)"%(iEX)

            end_time = time.time()        
            # contains number of started replicas
            numReplica = len(self.replica_jobs)
    
            print "\n started " + "%d"%numReplica + " of " + str(self.total_number_replica) + " in this round." 
            print "\n Time for spawning " + "%d"%numReplica + " replica: " + str(end_time-start_time) + " s"

            #################################### Waiting for job termination #####################################
            # Start  job monitoring
            energy = [0 for i in range(0, numReplica)]
            flagJobDone = [ False for i in range(0, numReplica)]
            numJobDone = 0

            print "\n" 
            while 1:
                   print "\n##################### Replica State Check at: " + time.asctime(time.localtime(time.time())) + " ########################"
                   for irep in range(0, numReplica):
                       running_job = self.replica_jobs[irep]
                       try:
                           state = running_job.get_state()
                       except: 
                           pass
                       print "replica_id: " + str(irep) + " job: " + str(running_job) + "received_state: " + str(state) + " Time since launch: " + str(time.time()-start) + " sec"
                       if (str(state) == "Done") and (flagJobDone[irep] is False):
                           print "(INFO) Replica " + "%d"%irep + " done"
                           energy[irep] = self.get_energy(irep) ##todo get energy from right host
                           flagJobDone[irep] = True
                           numJobDone = numJobDone + 1
                           total_number_of_namd_jobs = total_number_of_namd_jobs + 1
                       elif(str(state)=="Failed"):
                           self.stop_glidin_jobs()
                           sys.exit(1)
                
                   if numJobDone == numReplica:
                           break
                   time.sleep(15)
            ####################################### Replica Exchange ##################################    
            # replica exchange step        
            print "\n(INFO) Now exchange step...."
            for irep in range(0, numReplica-1):
                en_a = energy[irep]
                en_b = energy[irep+1]
                self.do_exchange(energy, irep, irep+1)
    
            iEX = iEX +1
            output_str = "%5d-th EX :"%iEX
            for irep in range(0, numReplica):
                output_str = output_str + "  %s"%self.temperatures[irep]
            
            print "\n\nExchange result : "
            print output_str + "\n\n"
            
            ofile = open(ofilename,'a')
            for irep in range(0, numReplica):
                ofile.write(" %s"%(self.temperatures[irep]))
            ofile.write(" \n")            
            ofile.close()
    
            if iEX == numEX:
                break
        
       print "REMD Runtime: " + str(time.time()-start) + " sec; Pilot URL: " + str(self.bj.pilot_url) + "; number replica: " + str(self.total_number_replica) + "; number namd jobs: " + str(total_number_of_namd_jobs)

       print "\n (INFO) Stopping BigJob"  
       self.stop_bigjob()
       
#####################################################################
# main
#####################################################################
if __name__ == "__main__":
   #pdb.set_trace()
   start = time.time()
   op = optparse.OptionParser()
   op.add_option('--type','-t', default='REMD')
   op.add_option('--configfile','-c')
   op.add_option('--numreplica','-n',default='2')
   options, arguments = op.parse_args()

   if options != None and options.configfile!=None and options.type !=None and options.type in ("REMD"):
      re_manager = ReManager(options.configfile)
      try:
          re_manager.run_REMDg()
      except:
          traceback.print_exc(file=sys.stdout)
          print "Stop Glide-Ins"
          #re_manager.stop_bigjob()
   else:
      print "Usage : \n python " + sys.argv[0] + " --type=<REMD> --configfile=<configfile> \n"
      print "Example: \n python " + sys.argv[0] + " --type=REMD --configfile=remd.conf"
      sys.exit(1)      


