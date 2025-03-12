from collections import defaultdict
import copy
import time
import random

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
                new_value = max(0, topic['tsize'] + magnitude)
                topic['tsize'] = new_value
                # print(f"tsize for {identifier} increased to {topic['tsize']}")

    def increase_dfreq(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier:
                new_value = max(0, topic['dfreq'] + magnitude)
                topic['dfreq'] = new_value
                # print(f"dfreq for {identifier} increased to {topic['dfreq']}")

    def increase_message_drops(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier:
                new_value = max(0, topic['message_drops'] + magnitude)
                topic['message_drops'] = new_value
                # print(f"message_drops for {identifier} increased to {topic['message_drops']}")

    def increase_state_severity(self, identifier, magnitude):
        for process in self.processes:
            if process['hname'] == identifier or process['pid'] == identifier:
                new_value = max(0, process['state_severity'] + magnitude)
                process['state_severity'] = new_value
                # print(f"state_severity for {identifier} increased to {process['state_severity']}")

    def increase_state_severity_level(self, identifier, magnitude):
        for process in self.processes:
            if process['hname'] == identifier or process['pid'] == identifier:
                new_value = max(0, process['state_severity_level'] + magnitude)
                process['state_severity_level'] = new_value
                # print(f"state_severity_level for {identifier} increased to {process['state_severity_level']}")

    
    def add_process(self, hname, pid, uname, state_severity, state_severity_level, layer = "udp"):
        # print("Adding process")
        new_process = {
        "hname": hname,
        "pid": pid,
        "uname": uname,
        "state_severity": state_severity,
        "state_severity_level": state_severity_level,
        layer + "_transport_domain": hname,
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
        # print("Deleting process")
        self.processes = [process for process in self.processes if process['pid'] != pid]
        self.ecal_data["processes"] = self.processes
        
    
    def add_topic(self, hname, tname, pid, uname, tid, direction, tsize, dfreq, layer = "udp"):
        # print("Adding topic")
        new_topic = {
            "rclock": 15,
            "hname": hname,
            layer + "_transport_domain": hname,
            "pid": pid,
            "pname": "/usr/bin/ecal_mma-5.13.3",
            "uname": uname,
            "tid": tid,
            "tname": tname,
            "direction": direction,
            "tdatatype_name": "eCAL.pb.mma.State",
            "tdatatype_encoding": "proto",
            "tdatatype_descriptor": "ABC",
            "layer": [
                {
                    "type": "tl_ecal_udp_mc",
                    "version": 0,
                    "active": layer == "udp"
                },
                {
                    "type": "tl_ecal_shm",
                    "version": 0,
                    "active": layer == "shm"
                },
                {
                    "type": "tl_ecal_tcp",
                    "version": 0,
                    "active": layer == "tcp"
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
        # print("Delete topic")
        self.topics = [topic for topic in self.topics if topic['tid'] != tid]
        self.ecal_data["topics"] = self.topics 
        
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
            # print(f"Updated cpu_load for hname={hname}, pid={pid}: {cpu_load}")
        else:
            self.process_performances[hname][pid] = process_performance
            # print(f"Added new process performance for hname={hname}, pid={pid}")
    
    def delete_process_performance(self, pid):
        for host in self.process_performances:
            for host_pid in self.process_performances[host]:
                if host_pid == pid:
                    del self.process_performances[host][pid]
                    # print("Deleting process performance")
            
    
    def add_host(self, hname, cpu_load, total_memory, available_memory, capacity_disk, available_disk):
        # print("Adding new host ", hname)
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
    
    def change_host_cpu_load(self, hname, cpu_load):
        self.hosts[hname]["cpu_load"] = cpu_load
        # print("Changed cpu_load for ", hname)
        
        
    def delete_host(self, hname):
        del self.hosts[hname] 
        # print(f"Host '{hname}' has been removed.")
        self.ecal_data["hosts"] = self.hosts 
                
    def run_script(self):
        print("Running script...")
        self.reset()
        
        # time.sleep(5)
        self.add_host("RearLight", 0, 100, 100, 100, 100)
        self.add_process("RearLight", 21, "MMA_RearLight", 1, 0, "udp")
        # self.add_process("RearLight", 22, "sub_RearLight", 0, 1, "udp")
        self.add_process("RearLight", 23, "sub_RearLight", 0, 1, "udp")
        self.add_topic("RearLight", "ms_RearLight", 21, "MMA_RearLight", "21", "publisher", 1000, 1000, "udp")
        # self.add_topic("RearLight", "ms_RearLight", 22, "sub_RearLight", "22", "subscriber", 1000, 1000, "udp")
        self.add_topic("RearLight", "ms_HPC", 23, "sub_RearLight", "23", "subscriber", 1000, 1000, "udp")
        
        # time.sleep(5)
        
        self.add_host("Brakes", 0, 100, 100, 100, 100)
        self.add_process("Brakes", 31, "MMA_Brakes", 1, 0, "udp")
        # self.add_process("Brakes", 32, "sub_Brakes", 0, 1, "udp")
        self.add_process("Brakes", 33, "sub_Brakes", 0, 1, "udp")
        self.add_topic("Brakes", "ms_Brakes", 31, "MMA_Brakes", "31", "publisher", 1000, 1000, "udp")
        # self.add_topic("Brakes", "ms_Brakes", 32, "sub_Brakes", "32", "subscriber", 1000, 1000, "udp")
        self.add_topic("Brakes", "ms_HPC", 33, "sub_Brakes", "33", "subscriber", 1000, 1000, "udp")
        
        
        # time.sleep(5)
               
        self.add_process("HPC", 13, "sub_HPC", 0, 1, "udp")
        self.add_process("HPC", 14, "sub_HPC", 0, 1, "udp")
        self.add_topic("HPC", "ms_RearLight", 13, "sub_HPC", "13", "subscriber", 1000, 1000, "udp")
        self.add_topic("HPC", "ms_Brakes", 14, "sub_HPC", "14", "subscriber", 1000, 1000, "udp")
        
        
        for i in range(5):
            time.sleep(5)
            val = 100 - 2*i
            self.increase_tsize(21, 500)
            self.increase_state_severity(21, 1)
            self.change_host_cpu_load("HPC", 20)
            self.add_host("HPC", 50+10*i, 100, val, 100, val-10)
            self.increase_message_drops(33, i)
            self.update_process_performance("RearLight", 33, 50+5*i)
            self.update_process_performance("RearLight", 23, 20+5*i)
            self.update_process_performance("HPC", 13, 30+15*i)
        
        time.sleep(5)
        self.delete_topic("33")
        self.delete_topic("23")
        # self.delete_process_performance(33)
        time.sleep(5)
        
        
        
        # Schleife mit der zufällige Message Drops erzeugt werden
        # for _ in range(20):
        #     random_message_drops = random.randint(-20, 50)
        #     pid_list = [topic["pid"] for topic in self.topics if topic["direction"] == "subscriber"]
        #     pid = random.choice(pid_list)
        #     self.increase_message_drops(pid, random_message_drops)
        #     time.sleep(2)
                    
        
        
        # Schleife mit der weitere HPC publisher erzeugt werden
        # for id in range(15, 19):  
        #     self.add_process("HPC", id, "MMA_HPC "+ str(id), 0, 1)
        #     self.add_topic("HPC", "ms_HPC", id, "MMA_HPC " + str(id), str(id), "publisher", 1000, 1000)
        
        time.sleep(1)

        print("Script finished.")
        