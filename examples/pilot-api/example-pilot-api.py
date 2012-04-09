import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.getcwd() + "/../")
from pilot import PilotComputeService, ComputeDataService, State

if __name__ == "__main__":      
    
    pilot_compute_service = PilotComputeService()

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": os.path.join(os.getcwd(),"work"),
                             'affinity_datacenter_label': "eu-de-south",              
                             'affinity_machine_label': "mymachine" 
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    pilotjob2 = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
         
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    
    # start work unit
    compute_unit_description = {
            "executable": "/bin/sleep",
            "arguments": ["100"],
            "total_core_count": 1,
            "number_of_processes": 1,            
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "affinity_datacenter_label": "eu-de-south",              
            "affinity_machine_label": "mymachine" 
    }    
    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    
    
    compute_data_service.wait()
    
    logging.debug("Finished setup. Waiting for scheduling of CU")
    
    logging.debug("Terminate Pilot Compute and Compute Data Service")
    compute_data_service.cancel()    
    pilot_compute_service.cancel()
