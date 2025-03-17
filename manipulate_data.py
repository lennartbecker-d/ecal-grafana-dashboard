from collections import defaultdict
import copy
import time
import threading
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
        # self.logs = self.ecal_data.get("logs", {})  
    def reset(self):
        print("reset data")
        self.ecal_data = copy.deepcopy(self.ecal_raw_data)
        self.processes = self.ecal_data.get("processes", {})
        self.services = self.ecal_data.get("services", {})
        self.clients = self.ecal_data.get("clients", {})
        self.topics = self.ecal_data.get("topics", {})
        self.hosts = self.ecal_data.get("hosts", {})
        self.process_performances = self.ecal_data.get("process_performances", {})
        # self.logs = self.ecal_data.get("logs", {})
    
    def return_data(self):
        return copy.deepcopy(self.ecal_data)
    
    def set_tsize(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier or topic['tname'] == identifier or topic['uname'] == identifier:
                topic['tsize'] = magnitude
                # print(f"tsize for {identifier} increased to {topic['tsize']}")

    def set_dfreq(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier or topic['tname'] == identifier or topic['uname'] == identifier:
                topic['dfreq'] = magnitude
                # print(f"dfreq for {identifier} increased to {topic['dfreq']}")

    def set_message_drops(self, identifier, magnitude):
        for topic in self.topics:
            if topic['tid'] == identifier or topic['hname'] == identifier or topic['pid'] == identifier or topic['tname'] == identifier or topic['uname'] == identifier:
                topic['message_drops'] = magnitude
                # print(f"message_drops for {identifier} increased to {topic['message_drops']}")

    def set_state_severity(self, identifier, magnitude):
        for process in self.processes:
            if process['hname'] == identifier or process['pid'] == identifier or process['uname'] == identifier:
                # new_value = max(0, process['state_severity'] + magnitude)
                process['state_severity'] = magnitude
                # print(f"state_severity for {identifier} increased to {process['state_severity']}")

    def set_state_severity_level(self, identifier, magnitude):
        for process in self.processes:
            if process['hname'] == identifier or process['pid'] == identifier or process['uname'] == identifier:
                # new_value = max(0, process['state_severity_level'] + magnitude)
                process['state_severity_level'] = magnitude
                # print(f"state_severity_level for {identifier} increased to {process['state_severity_level']}")
    
    def update_severity(self):
        for process in self.processes:
            pid = process["pid"]
            hname = process["hname"]
            
            # retrieve total_memory
            for host in self.hosts.values():
                if host["hname"] == hname:
                    total_memory = host["total_memory"]
            # retrieve size of package
            for topic in self.topics:
                if topic["pid"] == pid:
                    tsize = topic["tsize"]
            
            
            if tsize <= 0.4*total_memory:
                severity = 1  # healthy
                level = self.determine_level(tsize, 0, 0.4*total_memory)
            elif tsize <= 0.6*total_memory:
                severity = 2  # warning
                level = self.determine_level(tsize, 0.4*total_memory, 0.6*total_memory)
            elif tsize <= 0.8*total_memory:
                severity = 3  # critical
                level = self.determine_level(tsize, 0.6*total_memory, 0.8*total_memory)
            else:
                severity = 4  # failed
                level = self.determine_level(tsize, 0.8*total_memory, total_memory)

            self.set_state_severity(pid, severity)
            self.set_state_severity_level(pid, level)

    def determine_level(self, size, min_size, max_size):
        interval = (max_size - min_size) / 5
        if size <= min_size + interval:
            return 1  # level1
        elif size <= min_size + 2 * interval:
            return 2  # level2
        elif size <= min_size + 3 * interval:
            return 3  # level3
        elif size <= min_size + 4 * interval:
            return 4  # level4
        else:
            return 5  # level5
    
    def update_rclock(self):
        rclock = int(time.time()- self.start_time)
        for process in self.processes:
            process["rclock"] = rclock
        for topic in self.topics:
            topic["rclock"] = rclock
    
    def add_process(self, hname, pid, uname, state_severity, state_severity_level, layer = "udp"):
        # print("Adding process")
        new_process = {
        "hname": hname,
        "pid": pid,
        "uname": uname,
        "state_severity": state_severity,
        "state_severity_level": state_severity_level,
        layer + "_transport_domain": hname,
        "rclock": 0, 
        "pname": "/usr/bin/ecal_mma-6.0.0-rc",
        "pparam": "ecal_mma-6.0.0-rc",
        "state_info": "Running",
        "tsync_state": 0,
        "tsync_mod_name": "",
        "component_init_state": 129,
        "component_init_info": "pub",
        "ecal_runtime_version": "v6.0.0-rc.1-23-g21abac1cc"
    }
        self.processes.append(new_process)
    
    def delete_process(self, pid):
        # print("Deleting process")
        self.processes = [process for process in self.processes if process['pid'] != pid]
        self.ecal_data["processes"] = self.processes
        
    
    def add_topic(self, hname, tname, pid, uname, tid, direction, tsize, dfreq, layer = "udp", icon="add-user"):
        # print("Adding topic")
        new_topic = {
            "rclock": 0,
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
            "dfreq": dfreq,
            "icon": icon,
        }
        self.topics.append(new_topic)
        if pid not in self.process_performances:
            self.create_process_performance(hname, pid, 0)   # default CPU load
    
    def delete_topic(self, tid):
        # print("Delete topic")
        self.topics = [topic for topic in self.topics if topic['tid'] != tid]
        self.ecal_data["topics"] = self.topics 
        
    
    def create_process_performance(self, hname, pid, cpu_load):      
        process_performance = {
            "hname": hname,
            "pid": pid,
            "current_working_set_size": 0,
            "peak_working_set_size": -1,
            "cpu_kernel_time": -1,
            "cpu_user_time": -1,
            "cpu_creation_time": -1,
            "cpu_load": cpu_load
        }
        if hname not in self.process_performances:
            self.process_performances[hname] = {}
            
        self.process_performances[hname][pid] = process_performance
    
    def update_process_performance(self):      
        for topic in self.topics:
            tsize = topic["tsize"]
            hname = topic["hname"]
            pid = topic["pid"]
            for host in self.hosts.values():
                if host["hname"] == hname:
                    cpu_load = tsize / host["total_memory"] * 100
                    
            self.process_performances[hname][pid]["cpu_load"] = cpu_load
            self.process_performances[hname][pid]["current_working_set_size"] = tsize
            

    
    def delete_process_performance(self, pid):
        for host in self.process_performances:
            for host_pid in self.process_performances[host]:
                if host_pid == pid:
                    del self.process_performances[host][pid]
                    # print("Deleting process performance")
            
    
    def add_host(self, hname, cpu_load, total_memory, available_memory, capacity_disk, available_disk, icon):
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
                "num_cpu_cores": 20,
                "icon": icon,
                "logged_state":{
                                "<60": False,
                                "<80": False,
                                ">=80": False
                            }
            }
        self.hosts[hname] = new_host
    
    def update_host_performance(self):
        for host in self.hosts.values():
            tsize = 0
            # Add all incoming and outgoing packages
            for topic in self.topics:
                if host["hname"] == topic["hname"]:
                    tsize += topic["tsize"]
            
            cpu_load = min(100, tsize/host["total_memory"] * random.randint(70, 130))
            available_memory = max(0, host["total_memory"] - tsize)
            available_disk = max(0, host["capacity_disk"]- tsize) 
            
            host["cpu_load"] = cpu_load
            host["available_memory"] = available_memory
            host["available_disk"] = available_disk
            
            if cpu_load < 40:
                # Resetting logged_state if cpu_load is less than 40
                host["logged_state"] = {
                    "<60": False,
                    "<80": False,
                    ">=80": False
                }
            elif cpu_load < 60 and not host["logged_state"]["<60"]:
                # self.logs.append({
                #     "message": f"[CPU SATURATION] investigate host {host["hname"]}",
                #     "level": "warning",
                # })
                host["logged_state"] = {
                    "<60": True,
                    "<80": False,
                    ">=80": False
                }
            elif cpu_load < 80 and not host["logged_state"]["<80"]:
                # self.logs.append({
                #     "message": f"[CPU OVERLOAD] loosing host {host["hname"]}",
                #     "level": "critical",
                # })
                host["logged_state"] = {
                    "<60": True,
                    "<80": True,
                    ">=80": False
                }
            elif not host["logged_state"][">=80"]:
                # self.logs.append({
                #     "message": f"[CPU FAILURE] host {host["hname"]} is not working properly",
                #     "level": "error",
                # })
                host["logged_state"] = {
                    "<60": True,
                    "<80": True,
                    ">=80": True
                }
        
    def delete_host(self, hname):
        del self.hosts[hname] 
        # print(f"Host '{hname}' has been removed.")
        self.ecal_data["hosts"] = self.hosts 



###########################################################################################################################################
#                                                           INITIAL SETUP
###########################################################################################################################################                
    def initial_setup(self):
        # --- Add Hosts ---
        self.add_host(
            hname="CamGrabber",
            cpu_load=0,
            total_memory=1.9252312e7,
            available_memory=1.9252312e7,
            capacity_disk=18.7582392e7,
            available_disk=18.7582392e7,
            icon = "camera"
        )

        self.add_host(
            hname="LaneRecognition",
            cpu_load=0,
            total_memory=3.61283654e7,
            available_memory=3.61283654e7,
            capacity_disk=44.13467724e7,
            available_disk=44.13467724e7,
            icon = "calculator-alt"
        )
        
        self.add_host(
            hname="HMI",
            cpu_load=0,
            total_memory=1.54435343e7,
            available_memory=1.54435343e7,
            capacity_disk=25.12352323e7,
            available_disk=25.12352323e7,
            icon = "monitor"
        )
        
        self.add_host(
            hname="PathPlanning",
            cpu_load=0,
            total_memory=2.345432534e7,
            available_memory=2.345432534e7,
            capacity_disk=70.12312445e7,
            available_disk=70.12312445e7,
            icon = "multi-step"
        )

        # --- Add Processes ---
        
        self.add_process(
            hname="CamGrabber",
            pid=12,
            uname="SendImages",
            state_severity=1,
            state_severity_level=1,
            layer="udp"
        )
        
        self.add_process(
            hname="LaneRecognition",
            pid=21,
            uname="ReceiveImages",
            state_severity=1,
            state_severity_level=1,
            layer="udp"
        )
        
        self.add_process(
            hname="LaneRecognition",
            pid=23,
            uname="SendVisualization",
            state_severity=1,
            state_severity_level=1,
            layer="udp"
        )
        
        self.add_process(
            hname="HMI",
            pid=32,
            uname="ReceiveVisualization",
            state_severity=1,
            state_severity_level=1,
            layer="udp"
        )
        
        self.add_process(
            hname="LaneRecognition",
            pid=34,
            uname="SendPathInformation",
            state_severity=1,
            state_severity_level=1,
            layer="udp"
        )
        
        self.add_process(
            hname="PathPlanning",
            pid=43,
            uname="ReceivePathInformation",
            state_severity=1,
            state_severity_level=1,
            layer="udp"
        )
        
        # --- Add Topics ---
        
        self.add_topic(
            hname="CamGrabber",
            tname="TransferImages",
            pid=12,
            uname="Images",
            tid="T100",
            direction="publisher",
            tsize=0,
            dfreq=1000000,
            layer="udp",
            icon="camera"
        )
        
        self.add_topic(
            hname="LaneRecognition",
            tname="TransferImages",
            pid=21,
            uname="Images",
            tid="T100_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000000,
            layer="udp",
            icon="calculator-alt"
        )
        
        self.add_topic(
            hname="LaneRecognition",
            tname="TransferVisualization",
            pid=23,
            uname="Visualization",
            tid="T200",
            direction="publisher",
            tsize=0,
            dfreq=1000000,
            layer="udp",
            icon="calculator-alt"
        )
        
        self.add_topic(
            hname="HMI",
            tname="TransferVisualization",
            pid=32,
            uname="Visualization",
            tid="T200_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000000,
            layer="udp",
            icon="monitor"
        )
        
        self.add_topic(
            hname="LaneRecognition",
            tname="TransferPathInformation",
            pid=24,
            uname="Path",
            tid="T300",
            direction="publisher",
            tsize=0,
            dfreq=1000000,
            layer="udp",
            icon="calculator-alt"
        )
        
        self.add_topic(
            hname="PathPlanning",
            tname="TransferPathInformation",
            pid=42,
            uname="Path",
            tid="T300_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000000,
            layer="udp",
            icon="multi-step"
        )

###########################################################################################################################################
#                                                           RUN SCRIPT
###########################################################################################################################################
    
    def send_images(self, size):
        self.update_rclock()
        process_ok = True
        if size > 600000:
            process_ok = False
        
        size2 = size
        
        if process_ok:
            self.set_tsize("Images", size)
        else:
            size2 = size*0.98
            self.set_tsize(12, size)
            self.set_tsize(21, size2)
            
        self.send_visualization(size2, process_ok)
        self.create_path(size2, process_ok)
            
        self.update_host_performance()
        self.update_severity()
        self.update_process_performance()
        self.set_message_drops(21, (size-size2)/100000)
    
    def send_visualization(self, size_images, process_ok):
        size = size_images/2 * max(0.1, random.normalvariate(1, 0.3))
        # size = max(500, min(size, 10000))

        size2 = size
        
        if process_ok:
            self.set_tsize("Visualization", size)
        else:
            size2 = size*0.98
            self.set_tsize(23, size)
            self.set_tsize(32, size2)
            
        self.set_message_drops(32, (size-size2)/2000)
    
    def create_path(self, size_images, process_ok):
        size = size_images/3 * max(0.1, random.normalvariate(1, 0.4))
        # size = max(100, min(size, 7000))
        
        size2 = size
        
        if process_ok:
            self.set_tsize("Path", size)
        else:
            size2 = size*0.98
            self.set_tsize(24, size)
            self.set_tsize(42, size2)
            
        self.set_message_drops(42, (size-size2)/6000)
        
    
    def escalate_overloading(self):
        self.set_message_drops(15, 5)
        self.set_message_drops(33, 5)
        self.set_tsize(12, 3000)
        self.set_tsize(13, 3000)
        self.set_state_severity_level(16, 4)
        self.set_state_severity(16, 5)
        self.delete_host("Brakes")
        self.add_host(
            hname="HPC",
            cpu_load=100,
            total_memory=100,
            available_memory=0,
            capacity_disk=100,
            available_disk=0
        )
        time.sleep(10)
        self.delete_topic("T200")
        self.delete_topic("T200_sub")
        time.sleep(15)
        self.set_tsize(12, 3000)
        self.set_tsize(13, 3000)
        self.delete_host("TailLight")
        self.delete_topic("T100")
        self.delete_topic("T100_sub")
        self.delete_topic("T400")
        self.delete_topic("T400_sub")
        time.sleep(10)
        self.initial_setup()
        
        
    def run_script(self):
        self.start_time = time.time()
        self.reset()
        self.initial_setup()
        time.sleep(10)
        
        # Running healthy
        for _ in range(3):
            size = random.normalvariate(200e3, 60e3)
            size = max(50e3, min(size, 0.25*self.hosts["CamGrabber"]["total_memory"]))
            self.send_images(size)
            time.sleep(5)
        
        print("Increase overloading 1")
        # Increase overloading 1
        for _ in range(5):
            size = random.normalvariate(0.2*self.hosts["CamGrabber"]["total_memory"], 0.06*self.hosts["CamGrabber"]["total_memory"])
            size = max(0.1*self.hosts["CamGrabber"]["total_memory"], 0.35* min(size, 0.2*self.hosts["CamGrabber"]["total_memory"]))
            self.send_images(size)
            time.sleep(5)
        
        # Increase overloading 2
        print("Increase overloading 2")
        
        for _ in range(5):
            size = random.normalvariate(0.7*self.hosts["CamGrabber"]["total_memory"], 0.1*self.hosts["CamGrabber"]["total_memory"])
            size = max(0.5*self.hosts["CamGrabber"]["total_memory"], min(size, self.hosts["CamGrabber"]["total_memory"]))
            self.send_images(size)
            time.sleep(5)
            
        print("Escalate")
        for _ in range(10):
            size = random.normalvariate(0.95*self.hosts["CamGrabber"]["total_memory"], 0.03*self.hosts["CamGrabber"]["total_memory"])
            size = max(0.8*self.hosts["CamGrabber"]["total_memory"], min(size, self.hosts["CamGrabber"]["total_memory"]))
            self.send_images(size)
            time.sleep(5)

        
        # self.add_overloading_process()
        # time.sleep(10)
        
        # # start_time =time.time()
        # # while time.time() - start_time < 300:
        # brake_state_thread = threading.Thread(target=self.send_brake_state_ok)
        # tail_light_state_thread = threading.Thread(target=self.send_tail_light_state_ok)
        # tail_light_command_thread = threading.Thread(target=self.send_tail_light_command_ok)
        # overloading_thread = threading.Thread(target=self.increase_overloading)
        # brake_state_thread.start()
        # tail_light_state_thread.start()
        # tail_light_command_thread.start()
        # overloading_thread.start()
        
        
        