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
import subprocess

from bigjob import bigjob, subjob, description


class ReManager():

   ############################################################################
   #  This class hold information about the application and replicas runnning 
   ###########################################################################
   def __init__(self, config_filename):

       self.exchange_count = 0
       self.arguments = []
       self.replica_count = 0
       #self.replica_id = 0
       self.NUMBER_BIGJOBS = 0
       self.temperatures = []
       self.replica_jobs = []
       self.bjs=[]
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
       self.host1= default_dict["host1"]
       print "\n Host of the application is: " + self.host1
       self.host2= default_dict["host2"]
       print "\n Host of the application is: " + self.host2
       self.host3= default_dict["host3"]
       print "\n Host of the application is: " + self.host3
       self.cores_per_replica = default_dict["cores_per_replica"]
       print "\n number of cores per replica is: " + self.cores_per_replica
       self.temperatures = default_dict["temperature"].split()
       self.COORDINATION_URL = default_dict["coordination_url"]
       self.RPB = config.getint("DEFAULT" , "RPB")
       self.NUMBER_BIGJOBS = config.getint("DEFAULT" , "NUMBER_BIGJOBS")
 
   def start_bigjob(self,COORDINATION_URL,RESMGR_URL,i):

       #####################################################
       # Delete and Create BigJob Agent Working directory
       #####################################################

       try:
          #print "\n (Delete Remote Directory If Exists)" + "ssh "+ str(RESMGR_URL[10:])+ " rm -rf "+ self.working_directory+"async_agent_16"
          os.system(" ssh " + str(RESMGR_URL[10:]) + " rm -rf "+ self.working_directory + "sync_agent_16")
          print "\n (Success) Deleted BigJobAgent Working Directory"
          os.system(" ssh " + str(RESMGR_URL[10:]) + " mkdir "+ self.working_directory + "sync_agent_16")
          print "\n (Success) Created BigJobAgent Working Directory"
       except IOError, e:
          print 'exists', e

       ##########################################################################################
       # make sure you are familiar with the queue structure on futuregrid,ppn, your project id
       # and the walltime limits on each queue. change accordingly
       #
       queue="batch"           # Queue to which BigJob has to be submitted, if None, default queue is considered.
       project=None            # Allocation Information. if None, default information is considered
       walltime=120             # Time in minutes. There are limits on the time you can request

       processes_per_node=8    # ppn
       number_of_processes=64  # The total number of processes ( BigJob size), used to run Jobs
       workingdirectory= os.path.join(os.getcwd(), "sync_agent_16") # working directory for agent.
       ##########################################################################################
       #pdb.set_trace()
       #self.bjs=[]
       print "\n (VARIABLE) " + str(i)
       print "\n (INFO) Start Pilot Job/BigJob at: " + RESMGR_URL
       start= time.time()
       bj = bigjob(COORDINATION_URL)
       self.bjs.append(bj)
       self.bjs[i].start_pilot_job(RESMGR_URL,
                                   None,
                                   number_of_processes,
                                   queue,
                                   project,
                                   workingdirectory,
                                   None,
                                   walltime,
                                   processes_per_node)

       print "\n (INFO) Pilot Job/BigJob URL: " + self.bjs[i].pilot_url + " State: " + str(self.bjs[i].get_state())
       logging.debug("BigJob Initiation time: " + str(time.time()-start))
       print "\n (INFO) BigJob Initiation time: " + str(time.time()-start)
       return self.bjs
 

   def stage_in_files(self,replica_id,RESMGR_URL):
       start = time.time()
       #pdb.set_trace()
       print "\n (INFO) " + "scp -r " + self.replica_directory + "* " + str(RESMGR_URL)+ ":"+self.working_directory + "sync_agent_16/" + str(replica_id) + "/"
       print "\n (INFO) " + "scp -r " + self.working_directory + "sync_agent_16/ " + str(replica_id) + " " + str(RESMGR_URL)+ ":"+self.working_directory + "sync_agent/"
       i=replica_id
       """try:
           os.mkdir(self.working_directory + "sync_agent_16/" + str(replica_id))
       except:
           print "\n (INFO) Cannot create Directory  For replica_id :" + str(i)
       """ 
       if(i<self.RPB):
          try:
             os.mkdir(self.working_directory + "sync_agent_16/" + str(replica_id))
             os.system("cp -r " + self.replica_directory + "* " + self.working_directory + "sync_agent_16/" + str(replica_id) + "/")
             #print "\n (INFO) total time taken to stage files on " + str(RESMGR_URL) + "is : "+ str(time.time()-start)
          except:
             print "\n (INFO) Error" + str(RESMGR_URL) 

       elif((i>=self.RPB) and (i<=2*self.RPB)):
          try:
             os.system(" ssh " + str(RESMGR_URL) + " mkdir "+ self.working_directory + "sync_agent_16/"+ str(replica_id))
             os.system("scp -r " + self.replica_directory + "* " + str(RESMGR_URL)+ ":"+self.working_directory + "sync_agent_16/" + str(replica_id) + "/")
             #print "\n (INFO) total time taken to stage files on " + str(RESMGR_URL) + "is : "+ str(time.time()-start)
          except:
             print "\n (INFO) Error" + str(RESMGR_URL) 
        
       elif((i>=2*self.RPB) and (i<=3*self.RPB)):
          try:
             os.system(" ssh " + str(RESMGR_URL) + " mkdir "+ self.working_directory + "sync_agent_16/"+ str(replica_id))
             os.system("scp -r " + self.replica_directory + "* " + str(RESMGR_URL)+ ":"+self.working_directory + "sync_agent_16/" + str(replica_id) + "/")
             #print "\n (INFO) total time taken to stage files on " + str(RESMGR_URL) + "is : "+ str(time.time()-start)
          except:
             print "\n (INFO) Error" + str(RESMGR_URL) 
               
       else:
          try:
             os.system(" ssh " + str(RESMGR_URL) + " mkdir "+ self.working_directory + "sync_agent_16/"+ str(replica_id))
             os.system("scp -r " + self.replica_directory + "* " + str(RESMGR_URL)+ ":"+self.working_directory + "sync_agent_16/" + str(replica_id) + "/")
             #print "\n (INFO) total time taken to stage files on " + str(RESMGR_URL) + "is : "+ str(time.time()-start)
          except:
             print "\n (INFO) Error" + str(RESMGR_URL)
 
   def prepare_NAMD_config(self, replica_id,RESOURCEMGR_URL):
       # The idea behind this is that we can simply modify NPT.conf before submit a job to set temp and other variables
       start = time.time()
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
       print "\n (INFO) NAMD Prepared time on " + str(RESOURCEMGR_URL)+ " is: "+ str(time.time()-start)

   def transfer_NPT(self, replica_id,RESOURCEMGR_URL):
       start =  time.time()
       #print "\n (INFO) " + str(RESOURCEMGR_URL)
       #print "\n (INFO) " + "scp " + self.working_directory + "NPT.conf " + str(RESOURCEMGR_URL) + ":" +self.working_directory + "sync_agent_16/" + str(replica_id) + "/"
       try:    
          os.system("scp " + self.working_directory + "NPT.conf " + str(RESOURCEMGR_URL) + ":" +self.working_directory + "sync_agent_16/" + str(replica_id) + "/")
          print "\n (INFO) total time taken to transfer NPT on " + str(RESOURCEMGR_URL)+ " is: " + str(time.time()-start)
       except OSError:
          print "Unexpected error:", sys.exc_info() [0]

   def get_job_description(self, replica_id):        

       jd = description()  
       jd.executable = self.working_directory + "sync_agent_16/" + str(replica_id) + "/namd2"
       jd.number_of_processes = "4" 
       jd.spmd_variation = "single"
       jd.arguments = ["NPT.conf"] 
       jd.working_directory = self.working_directory + "sync_agent_16/" + str(replica_id) + "/"
       jd.output = "stdout-" + str(replica_id) + ".txt"
       jd.error = "stderr-" + str(replica_id) + ".txt"
       
       return jd

   def submit_subjob(self,replica_id, jd):
       #######  submit job via pilot job ######
       i=replica_id
       if(i < self.RPB):
            k=0     
            sj = subjob()
            sj.submit_job(self.bjs[k].pilot_url, jd)
            self.job_start_times[sj]=time.time()
            self.job_states[sj] = sj.get_state()
            return sj

       elif((i>=self.RPB) and (i<2*self.RPB)):
            k=1
            sj = subjob()
            sj.submit_job(self.bjs[k].pilot_url, jd)
            self.job_start_times[sj]=time.time()
            self.job_states[sj] = sj.get_state()
            return sj

       elif((i>=2*self.RPB) and (i<3*self.RPB)):
            k=2
            sj = subjob()
            sj.submit_job(self.bjs[k].pilot_url, jd)
            self.job_start_times[sj]=time.time()
            self.job_states[sj] = sj.get_state()
            return sj

       else:
            k=3
            sj = subjob()
            sj.submit_job(self.bjs[k].pilot_url, jd)
            self.job_start_times[sj]=time.time()
            self.job_states[sj] = sj.get_state()
            return sj

   def get_energy(self, replica_id):
       """ parse energy out of stdout """
       #pdb.set_trace() 
       i=replica_id
       print "\n (INFO) Get Energy: " + str(replica_id)
       if(i< self.RPB): 
           ssh = subprocess.Popen(['ssh', 'ssarip1@sierra.futuregrid.org', 'cat' , self.working_directory+ "sync_agent_16/"+ str(replica_id) + "/stdout-" + str(replica_id) + ".txt"], stdout=subprocess.PIPE)         
           stdoutfile= ssh.stdout.readlines()
           for line in stdoutfile:
               items = line.split()
               if len(items) > 0:
                   if items[0] in ("ENERGY:"):
                      en = items[11]
           print "(DEBUG) energy : " + str(en) + " from replica " + str(replica_id)
           return eval(en)

       elif((i>=self.RPB) and (i<2*self.RPB)):
           ssh = subprocess.Popen(['ssh', 'ssarip1@india.futuregrid.org', 'cat' , self.working_directory+ "sync_agent_16/"+ str(replica_id) + "/stdout-" + str(replica_id) + ".txt"], stdout=subprocess.PIPE)          
           stdoutfile = ssh.stdout.readlines()
           for line in stdoutfile:
               items = line.split()
               if len(items) > 0:
                   if items[0] in ("ENERGY:"):
                      en = items[11]
           print "(DEBUG) energy : " + str(en) + " from replica " + str(replica_id)
           return eval(en)

       elif((i>=2*self.RPB) and (i<3*self.RPB)):
           ssh = subprocess.Popen(['ssh', 'ssarip1@alamo.futuregrid.org', 'cat' , self.working_directory+ "sync_agent_16/"+ str(replica_id) + "/stdout-" + str(replica_id) + ".txt"], stdout=subprocess.PIPE)          
           stdoutfile = ssh.stdout.readlines()
           for line in stdoutfile:
               items = line.split()
               if len(items) > 0:
                   if items[0] in ("ENERGY:"):
                      en = items[11]
           print "(DEBUG) energy : " + str(en) + " from replica " + str(replica_id)
           return eval(en)

       else:
           ssh = subprocess.Popen(['ssh', 'ssarip1@hotel.futuregrid.org', 'cat' , self.working_directory+ "sync_agent_16/"+ str(replica_id) + "/stdout-" + str(replica_id) + ".txt"], stdout=subprocess.PIPE)          
           stdoutfile = ssh.stdout.readlines()
           for line in stdoutfile:
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
       #print "\n (INFO) " + str(self.NUMBER_BIBJOBS)
       for i in range(0,1):
          self.bjs[i].cancel()
          print "\n (INFO)" + "Stopping bigjob at pilot url " + str(self.bjs[i].pilot_url)
    
 
   ############################################################################
   # run_REMDg
   ############################################################################
   def run_REMDg(self):
        
      ###### Main loop which runs the replica-exchange  ####
       REMD_start = time.time()
       COORDINATION_URL= self.COORDINATION_URL
       RESOURCEMGR_URL0= self.host
       RESOURCEMGR_URL1= self.host1
       RESOURCEMGR_URL2= self.host2
       RESOURCEMGR_URL3= self.host3
       RPB= self.RPB
       NUMBER_BIGJOBS= self.NUMBER_BIGJOBS
       numEX = self.exchange_count
       ofilename = "sync-remd-temp.out"
       #pdb.set_trace()
       replica_id=0
       
       for i in range(0,NUMBER_BIGJOBS):
           if(i==0):
              #print "\n (INFO) Start BigJob" + " at " + RESOURCEMGR_URL0
              b= self.start_bigjob(COORDINATION_URL,RESOURCEMGR_URL0,i)
              if b[i]==None or b[i].get_state()=="Failed":
                 return
           elif(i==1):
              #print "\n (INFO) Start BigJob" + " at " + RESOURCEMGR_URL1
              b= self.start_bigjob(COORDINATION_URL,RESOURCEMGR_URL1,i)
              if b[i]==None or b[i].get_state()=="Failed":
                 return
           elif(i==2):
              print "\n (INFO) Start BigJob" + " at " + RESOURCEMGR_URL2
              b= self.start_bigjob(COORDINATION_URL,RESOURCEMGR_URL2,i)
              if b[i]==None or b[i].get_state()=="Failed":
                 return
           else:
              #print "\n (INFO) Start BigJob" + " at " + RESOURCEMGR_URL3
              b= self.start_bigjob(COORDINATION_URL,RESOURCEMGR_URL3,i)
              if b[i]==None or b[i].get_state()=="Failed":
                 return
       ######################################################
       #   Stage Files
       ######################################################
       start_time = time.time()
       for i in range (0, self.total_number_replica):
           if(i<RPB):
             #pdb.set_trace()
             #print "\n (INFO) " + str(replica_id) + " " + str(RESOURCEMGR_URL0[10:])
             self.stage_in_files(replica_id,RESOURCEMGR_URL0[10:])
             replica_id = replica_id + 1
           elif((i>=RPB) and (i<2*RPB)):
             #print "\n (INFO) " + str(replica_id) + " " + str(RESOURCEMGR_URL1[10:])
             self.stage_in_files(replica_id,RESOURCEMGR_URL1[10:])
             replica_id = replica_id + 1 
           elif((i>=2*RPB) and (i<3*RPB)):
             self.stage_in_files(replica_id,RESOURCEMGR_URL2[10:])
             replica_id = replica_id + 1
           else:
             self.stage_in_files(replica_id,RESOURCEMGR_URL3[10:])
             replica_id = replica_id + 1
       end_time = time.time()
       print "\n (INFO) Total time to Stage files is: " + str(end_time-start_time)

       iEX = 0
       total_number_of_namd_jobs = 0
       while (iEX < numEX):
            print "\n"
            # reset replica number

            print "#################### spawn jobs ####################"
            self.replica_jobs = []
            #self.bigjob_states = []
            self.job_states= {}
            self.job_start_times = {}
            start_time = time.time()
            replica_id = 0
            k=0

            for k in range (0,NUMBER_BIGJOBS):
                #self.bigjob_states[k] = b[k].get_state()
                #print "\n (INFO) Variable value is: " + str(k)
                print "\n (INFO) BigJob State: " + str(b[k].get_state())
                pilot_url = self.bjs[k].pilot_url
                print " Pilot: " + pilot_url + "state: " + str(b[k].get_state())

                if str(b[k].get_state()) == "Running":
                   pass
                else:
                   pass

            print "\n (INFO) Total Replica length is: " + str(self.total_number_replica)
            logging.debug("pilot job running: " + str(self.total_number_replica) + "jobs.")
            job_spawn_time=time.time() 
            for i in range (0, self.total_number_replica):
                ############## replica job spawn ############
                #pdb.set_trace()
                #print "\n (INFO) Replica Variable value is: " + str(i)
                if(i< RPB):
                          #start1=time.time()
                          print "\n (INFO) " + str(RESOURCEMGR_URL0[10:])
                          #self.stage_in_files(replica_id,RESOURCEMGR_URL0[10:])
                          #print "\n (INFO) total time taken to stage files is: " + str(time.time()-start1)
                          self.prepare_NAMD_config(replica_id,RESOURCEMGR_URL0[10:])
                          self.transfer_NPT(replica_id,RESOURCEMGR_URL0[10:])
                          jd = self.get_job_description(replica_id)
                          new_job = self.submit_subjob(replica_id,jd)
                          self.replica_jobs.insert(replica_id, new_job)
                          replica_id = replica_id + 1
                          print "(INFO) Replica " + "%d"%replica_id + " started (Num of Exchange Done = %d)"%(iEX)
                          #end_time1 = time.time()
                          #print "\n Time for staging " +" replica: " + str(end_time1-start1) + " s"

                elif((i>=RPB) and (i<2*RPB)):
                          start2=time.time()
                          #print "\n (INFO) " + str(RESOURCEMGR_URL1[10:])
                          #self.stage_in_files(replica_id,RESOURCEMGR_URL1[10:])
                          #print "\n (INFO) total time taken to stage files is: " + str(time.time()-start2)
                          self.prepare_NAMD_config(replica_id,RESOURCEMGR_URL1[10:])
                          self.transfer_NPT(replica_id,RESOURCEMGR_URL1[10:])
                          jd = self.get_job_description(replica_id)
                          new_job = self.submit_subjob(replica_id,jd)
                          self.replica_jobs.insert(replica_id, new_job)
                          replica_id = replica_id + 1
                          print "(INFO) Replica " + "%d"%replica_id + " started (Num of Exchange Done = %d)"%(iEX)
                          #end_time2 = time.time()
                          #print "\n Time for staging " +" replica: " + str(end_time2-start2) + " s"

                elif((i>=2*RPB) and (i<3*RPB)):
                          start3=time.time()
                          #print "\n (INFO) " + str(RESOURCEMGR_URL2[10:])
                          #self.stage_in_files(replica_id,RESOURCEMGR_URL2[10:])
                          #print "\n (INFO) total time taken to stage files is: " + str(time.time()-start3)
                          self.prepare_NAMD_config(replica_id,RESOURCEMGR_URL2[10:])
                          self.transfer_NPT(replica_id,RESOURCEMGR_URL2[10:])
                          jd = self.get_job_description(replica_id)
                          new_job = self.submit_subjob(replica_id,jd)
                          self.replica_jobs.insert(replica_id, new_job)
                          replica_id = replica_id + 1
                          print "(INFO) Replica " + "%d"%replica_id + " started (Num of Exchange Done = %d)"%(iEX)
                          #end_time3 = time.time()
                          #print "\n Time for staging " +" replica: " + str(end_time3-start3) + " s"

                else:
                          start=time.time()
                          #print "\n (INFO) " + str(RESOURCEMGR_URL3[10:])
                          #self.stage_in_files(replica_id,RESOURCEMGR_URL3[10:])
                          #print "\n (INFO) total time taken to stage files is: " + str(time.time()-start)
                          self.prepare_NAMD_config(replica_id,RESOURCEMGR_URL3[10:])
                          self.transfer_NPT(replica_id,RESOURCEMGR_URL3[10:])
                          jd = self.get_job_description(replica_id)
                          new_job = self.submit_subjob(replica_id,jd)
                          self.replica_jobs.insert(replica_id, new_job)
                          replica_id = replica_id + 1
                          print "(INFO) Replica " + "%d"%replica_id + " started (Num of Exchange Done = %d)"%(iEX)
                          #end_time = time.time()
                          #print "\n Time for staging " +" replica: " + str(end_time-start) + " s"

            end_time=time.time()
            # contains number of started replicas
            numReplica = len(self.replica_jobs)
            print "\n started " + "%d"%numReplica + " of " + str(self.total_number_replica) + " in this round."
            print "\n Time for spawning " + "%d"%numReplica + " replica: " + str(end_time-job_spawn_time) + " s"

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
                       print "replica_id: " + str(irep) + " job: " + str(running_job) + "received_state: " + str(state) + " Time since launch: " + str(time.time()-job_spawn_time) + " sec"
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
            ####################################### Replica Exchange ##################################    
            # replica exchange step        
            print "\n(INFO) Now exchange step...."
            for irep in range(0, numReplica-1):
                en_a = energy[irep]
                en_b = energy[irep+1]
                self.do_exchange(energy, irep, irep+1)
    
            iEX = iEX + (numReplica/2)
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
        
       print "REMD Runtime: " + str(time.time()-REMD_start) + " sec; " + "number replica: " + str(self.total_number_replica) + "; number namd jobs: " + str(total_number_of_namd_jobs)
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
          re_manager.stop_bigjob()
   else:
      print "Usage : \n python " + sys.argv[0] + " --type=<REMD> --configfile=<configfile> \n"
      print "Example: \n python " + sys.argv[0] + " --type=REMD --configfile=remd.conf"
      sys.exit(1)      


