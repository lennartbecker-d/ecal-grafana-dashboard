from collections import defaultdict
import copy
import time

class ManipulateData:
    def __init__(self, ecal_raw_data):
        self.ecal_raw_data = copy.deepcopy(ecal_raw_data)
        self.ecal_data = ecal_raw_data
        
        self.processes = self.ecal_data.get("processes", {})
        self.services = self.ecal_data.get("services", {})
        self.clients = self.ecal_data.get("clients", {})
        self.topics = self.ecal_data.get("topics", {})
        self.hosts = self.ecal_data.get("hosts", {})
        self.process_performances = self.ecal_data.get("process_performances", {})     
    
    def reset(self):
        print("Reset data")
        self.ecal_data = copy.deepcopy(self.ecal_raw_data)
        self.processes = self.ecal_data.get("processes", {})
        self.services = self.ecal_data.get("services", {})
        self.clients = self.ecal_data.get("clients", {})
        self.topics = self.ecal_data.get("topics", {})
        self.hosts = self.ecal_data.get("hosts", {})
        self.process_performances = self.ecal_data.get("process_performances", {})
    
    def return_data(self):
        return copy.deepcopy(self.ecal_data)
    
    def increase_tsize(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier:
                topic['tsize'] += magnitude
                print(f"tsize for {identifier} increased to {topic['tsize']}")
        
    def increase_dfreq(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier:
                topic['dfreq'] += magnitude
                print(f"dfreq for {identifier} increased to {topic['dfreq']}")

    def increase_message_drops(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier:
                topic['message_drops'] += magnitude
                print(f"message_drops for {identifier} increased to {topic['message_drops']}")
        
    
    def increase_state_severity(self, identifier, magnitude):
        for process in self.processes:
            if process['hname'] == identifier or process['pid'] == identifier:
                process['state_severity'] += magnitude
                print(f"state_severity for {identifier} increased to {process['state_severity']}")
    
    def increase_state_severity_level(self, identifier, magnitude):
        for process in self.processes:
            if process['hname'] == identifier or process['pid'] == identifier:
                process['state_severity_level'] += magnitude
                print(f"state_severity_level for {identifier} increased to {process['state_severity_level']}")
    
    def add_process(self, hname, pid, uname, state_severity, state_severity_level):
        print("Adding process")
        new_process = {
        "hname": hname,
        "pid": pid,
        "uname": uname,
        "state_severity": state_severity,
        "state_severity_level": state_severity_level,
        "shm_transport_domain": hname,
        "rclock": 15, 
        "pname": "/usr/bin/ecal_mma-5.13.3",
        "pparam": "ecal_mma-5.13.3",
        "state_info": "Running",
        "tsync_state": 0,
        "tsync_mod_name": "",
        "component_init_state": 129,
        "component_init_info": "pub",
        "ecal_runtime_version": "v5.13.3"
    }
        self.processes.append(new_process)
    
    def delete_process(self, pid):
        print("Deleting process")
        self.processes = [process for process in self.processes if process['pid'] != pid]
        
    
    def add_topic(self, hname, pid, uname, tid, direction, tsize, dfreq):
        print("Adding topic")
        new_topic = {
            "rclock": 15,
            "hname": hname,
            "shm_transport_domain": hname,
            "pid": pid,
            "pname": "/usr/bin/ecal_mma-5.13.3",
            "uname": uname,
            "tid": tid,
            "tname": "machine_state_" + hname,
            "direction": direction,
            "tdatatype_name": "eCAL.pb.mma.State",
            "tdatatype_encoding": "proto",
            "tdatatype_descriptor": "ABC",
            "layer": [
                {
                    "type": "tl_ecal_udp_mc",
                    "version": 0,
                    "active": False
                },
                {
                    "type": "tl_ecal_shm",
                    "version": 0,
                    "active": True
                },
                {
                    "type": "tl_ecal_tcp",
                    "version": 0,
                    "active": False
                }
            ],
            "tsize": tsize,
            "connections_loc": 1,
            "connections_ext": 0,
            "message_drops": 0,
            "did": 0,
            "dclock": 17,
            "dfreq": dfreq
        }
        self.topics.append(new_topic)
        if pid not in self.process_performances:
            self.update_process_performance(hname, pid, 0.05)   # default CPU load
    
    def delete_topic(self, tid):
        print("Deleting topic")
        self.topics = [topic for topic in self.topics if topic['pid'] != tid]
    
    def update_process_performance(self, hname, pid, cpu_load):      
        process_performance = {
            "hname": hname,
            "pid": pid,
            "current_working_set_size": 9179136,
            "peak_working_set_size": -1,
            "cpu_kernel_time": -1,
            "cpu_user_time": -1,
            "cpu_creation_time": -1,
            "cpu_load": cpu_load
        }
        if hname not in self.process_performances:
            self.process_performances[hname] = {}
            
        existing_performance = self.process_performances[hname].get(pid)
        if existing_performance:
            existing_performance['cpu_load'] = cpu_load
            print(f"Updated cpu_load for hname={hname}, pid={pid}: {cpu_load}")
        else:
            self.process_performances[hname][pid] = process_performance
            print(f"Added new process performance for hname={hname}, pid={pid}")
    
    def delete_process_performance(self, hname, pid):
        print("Remove process performance")
        del self.hosts[hname][pid]
    
    def add_host(self, hname, cpu_load, total_memory, available_memory, capacity_disk, available_disk):
        print("Adding new host ", hname)
        new_host = {
                "hname": hname,
                "cpu_load": cpu_load,
                "total_memory": total_memory,
                "available_memory": available_memory,
                "capacity_disk": capacity_disk,
                "available_disk": available_disk,
                "network_send": -1,
                "network_receive": -1,
                "os": "LINUX Ubuntu 24.04.2 LTS",
                "num_cpu_cores": 20
            }
        self.hosts[hname] = new_host
        
    def delete_host(self, hname):
        del self.hosts[hname]
        print(f"Host '{hname}' has been removed.")
                
    def run_script(self):
        print("Running script...")
        self.reset()
        
        time.sleep(2)
        self.increase_tsize("Dummy1", 200)
        
        self.add_host("Dummy3", 0.05, 100, 70, 50, 45)
        
        self.add_process("Dummy3", 300001, "Dummy3", 0, 1)
        
        time.sleep(5)
        
        self.add_topic("Dummy3", 300001, "Dummy3", "21", "subscriber", 1000, 100)
        self.update_process_performance("Dummy3", 300001, 0.05)
        self.update_process_performance("Dummy3", 300002, 0.05)
        
        time.sleep(5)
        

        print("Script finished.")
        