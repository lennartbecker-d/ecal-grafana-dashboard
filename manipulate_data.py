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
            self.update_process_performance(hname, pid, 0)   # default CPU load
    
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
            existing_performance['cpu_load'] += cpu_load
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



###########################################################################################################################################
#                                                           INITIAL SETUP
###########################################################################################################################################                
    def initial_setup(self):
        # --- Add Hosts ---
        # HPC: Host where the central lighting controller and the faulty overload process run.
        self.add_host(
            hname="HPC",
            cpu_load=0,
            total_memory=100,
            available_memory=100,
            capacity_disk=100,
            available_disk=100
        )

        # Brakes: Host representing the brakes, sending brake status.
        self.add_host(
            hname="Brakes",
            cpu_load=0,
            total_memory=100,
            available_memory=100,
            capacity_disk=100,
            available_disk=100
        )

        # TailLight: Host for the tail light controller that subscribes to lighting commands.
        self.add_host(
            hname="TailLight",
            cpu_load=0,
            total_memory=100,
            available_memory=100,
            capacity_disk=100,
            available_disk=100
        )

        # --- Add Processes ---
        # HPC - Central Lighting Controller process (publishes lighting commands).
        self.add_process(
            hname="HPC",
            pid=11,
            uname="TailLightController",
            state_severity=0,
            state_severity_level=0,
            layer="udp"
        )
        
        # HPC - Overload Logger process (simulates a process that becomes overloaded and logs errors).
        self.add_process(
            hname="HPC",
            pid=12,
            uname="OverloadLogger",
            state_severity=0,
            state_severity_level=0,
            layer="udp"
        )
        
        # Brakes - Brake Sensor (publishes brake status data).
        self.add_process(
            hname="Brakes",
            pid=21,
            uname="BrakeSensor",
            state_severity=0,
            state_severity_level=0,
            layer="udp"
        )
        
        # TailLight - Tail Light Controller (subscribes to lighting commands).
        self.add_process(
            hname="TailLight",
            pid=32,
            uname="TailLightController",
            state_severity=0,
            state_severity_level=0,
            layer="udp"
        )
        
        # --- Add Topics ---
        # LightingCommand topic published by TailLightController (HPC).
        self.add_topic(
            hname="HPC",
            tname="LightingCommand",
            pid=11,
            uname="TailLightController",
            tid="T100",
            direction="publisher",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )
        
        # ErrorLogs topic published by the OverloadLogger (HPC).
        self.add_topic(
            hname="HPC",
            tname="ErrorLogs",
            pid=12,
            uname="OverloadLogger",
            tid="T300",
            direction="publisher",
            tsize=0,
            dfreq=1000,
            layer="shm"
        )
        
        # BrakeState topic published by BrakeSensor (Brakes).
        self.add_topic(
            hname="Brakes",
            tname="BrakeState",
            pid=21,
            uname="BrakeSensor",
            tid="T200",
            direction="publisher",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )
        
        # Publisher for LightingCommand on TailLight: Tail Light Controller sends lighting inquiries.
        self.add_topic(
            hname="TailLight",
            tname="TailLightState",
            pid=31,
            uname="TailLightController",
            tid="T400",
            direction="publisher",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )
        
        # ErrorLogs topic subscribed by the OverloadLogger (HPC).
        self.add_topic(
            hname="HPC",
            tname="ErrorLogs",
            pid=13,
            uname="OverloadLogger",
            tid="T300_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000,
            layer="shm"
        )
        
        # Subscriber for BrakeState on HPC: Central process or a dedicated subscriber to receive brake sensor data.
        self.add_topic(
            hname="HPC",
            tname="BrakeState",
            pid=14,
            uname="HPCBrakeSubscriber",
            tid="T200_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )
        
        # Subscriber for BrakeState on HPC: Central process or a dedicated subscriber to receive brake sensor data.
        self.add_topic(
            hname="HPC",
            tname="TailLightState",
            pid=15,
            uname="TailLightController",
            tid="T400_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )
        
        # Subscriber for LightingCommand on TailLight: Tail Light Controller receives lighting commands.
        self.add_topic(
            hname="TailLight",
            tname="LightingCommand",
            pid=32,
            uname="TailLightController",
            tid="T100_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )

###########################################################################################################################################
#                                                           RUN SCRIPT
###########################################################################################################################################
    def add_overloading_process(self):
        # Subscriber for BrakeState on HPC: Central process or a dedicated subscriber to receive brake sensor data.
        self.add_topic(
            hname="HPC",
            tname="OverloadingState",
            pid=16,
            uname="DisruptiveProcess",
            tid="T500_sub",
            direction="subscriber",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )
        
    # Publisher for Disruptive Process
        self.add_topic(
            hname="OverloadingProcess",
            tname="OverloadingState",
            pid=41,
            uname="DisruptiveProcess",
            tid="T500",
            direction="publisher",
            tsize=0,
            dfreq=1000,
            layer="udp"
        )
    
    # Brakes
    
    def send_brake_state_ok(self):
        for _ in range(24):
            self.increase_tsize(21, 1000)
            self.increase_tsize(14, 1000)
            self.update_process_performance("Brakes", 21, 10)
            self.update_process_performance("HPC", 14, 5)
            self.increase_state_severity("Brakes",1)
            self.increase_state_severity_level("HPC",1)
            time.sleep(5)
            self.increase_tsize(21, -1000)
            self.increase_tsize(14, -1000)
            self.update_process_performance("Brakes", 21, -10)
            self.update_process_performance("HPC", 14, -5)
            self.increase_state_severity("Brakes",-1)
            self.increase_state_severity_level("HPC",-1)
        self.send_brake_state_false()
    
    def send_brake_state_false(self):
        self.increase_tsize(21, 1000)
        self.increase_tsize(12, 1000)
        self.increase_tsize(13, 1000)
        self.update_process_performance("Brakes", 21, 10)
        self.update_process_performance("HPC", 12, 10)
        self.update_process_performance("HPC", 13, 10)
        self.increase_state_severity("Brakes",1)
        self.increase_state_severity_level("HPC",2)
        time.sleep(20)
        self.increase_tsize(21, -1000)
        self.increase_tsize(12, -1000)
        self.increase_tsize(13, -1000)
        self.update_process_performance("Brakes", 21, -10)
        self.update_process_performance("HPC", 12, -10)
        self.update_process_performance("HPC", 13, -10)
        self.increase_state_severity("Brakes",-1)
        self.increase_state_severity_level("HPC",-2)
        
    # Tail light
    
    def send_tail_light_state_ok(self):
        for _ in range(40):
            self.increase_tsize(31, 2000)
            self.increase_tsize(15, 2000)
            self.update_process_performance("TailLight", 31, 20)
            self.update_process_performance("HPC", 15, 5)
            self.increase_state_severity("TailLight",2)
            self.increase_state_severity_level("HPC",1)
            time.sleep(3)
            self.increase_tsize(31, -2000)
            self.increase_tsize(15, -2000)
            self.update_process_performance("TailLight", 31, -20)
            self.update_process_performance("HPC", 15, -5)
            self.increase_state_severity("TailLight",-2)
            self.increase_state_severity_level("HPC",-1)
        self.send_tail_light_state_false()
        
        
    def send_tail_light_state_false(self):
        self.increase_tsize(31, 2000)
        self.increase_tsize(12, 2000)
        self.increase_tsize(13, 2000)
        self.update_process_performance("TailLight", 31, 20)
        self.update_process_performance("HPC", 12, 15)
        self.update_process_performance("HPC", 13, 15)
        self.increase_state_severity("TailLight",1)
        self.increase_state_severity_level("HPC",1)
        time.sleep(20)
        self.increase_tsize(31, -2000)
        self.increase_tsize(12, -2000)
        self.increase_tsize(13, -2000)
        self.update_process_performance("TailLight", 31, -20)
        self.update_process_performance("HPC", 12, -15)
        self.update_process_performance("HPC", 13, -15)
        self.increase_state_severity("TailLight",-1)
        self.increase_state_severity_level("HPC",-1)
    
    def send_tail_light_command_ok(self):
        for _ in range(30):
            self.increase_tsize(11, 2000)
            self.increase_tsize(32, 2000)
            self.update_process_performance("TailLight", 32, 10)
            self.update_process_performance("HPC", 11, 10)
            self.increase_state_severity("TailLight",1)
            self.increase_state_severity_level("HPC",1)
            time.sleep(4)
            self.increase_tsize(11, -2000)
            self.increase_tsize(32, -2000)
            self.update_process_performance("TailLight", 32, -10)
            self.update_process_performance("HPC", 11, -10)
            self.increase_state_severity("TailLight",-1)
            self.increase_state_severity_level("HPC",-1)
        self.send_tail_light_command_false()
        
    def send_tail_light_command_false(self):
        self.increase_tsize(11, 2000)
        self.increase_tsize(12, 2000)
        self.increase_tsize(13, 2000)
        self.update_process_performance("HPC", 11, 20)
        self.update_process_performance("HPC", 12, 15)
        self.update_process_performance("HPC", 13, 15)
        self.increase_state_severity("TailLight",1)
        self.increase_state_severity_level("HPC",1)
        time.sleep(20)
        self.increase_tsize(11, -2000)
        self.increase_tsize(12, -2000)
        self.increase_tsize(13, -2000)
        self.update_process_performance("HPC", 11, -20)
        self.update_process_performance("HPC", 12, -15)
        self.update_process_performance("HPC", 13, -15)
        self.increase_state_severity("TailLight",-1)
        self.increase_state_severity_level("HPC",-1)
    
    # Overloading 
    
    def increase_overloading(self):
        time.sleep(20)
        for _ in range(7):
            magnitude = random.randint(300, 700)
            self.increase_tsize(41, magnitude)
            self.update_process_performance("OverloadingProcess", 41, magnitude/100)
            self.increase_tsize(16, magnitude)
            self.update_process_performance("HPC", 16, magnitude/100)
            time.sleep(10)
        for _ in range(3):
            magnitude = random.randint(300, 700)
            self.increase_tsize(41, magnitude)
            self.update_process_performance("OverloadingProcess", 41, magnitude/100)
            self.increase_tsize(16, magnitude)
            self.update_process_performance("HPC", 16, magnitude/100)
            self.increase_message_drops(13, random.randint(1,4))
            self.increase_message_drops(14, random.randint(1,4))
            self.increase_message_drops(15, random.randint(1,4))
            time.sleep(12)
        print("escalate overloading")
        self.escalate_overloading()
        self.reset()
        self.initial_setup()
        print("finished skript")
        
    def escalate_overloading(self):
        self.increase_message_drops(15, 5)
        self.increase_message_drops(14, 5)
        self.increase_tsize(12, 3000)
        self.increase_tsize(13, 3000)
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
        self.increase_tsize(12, 3000)
        self.increase_tsize(13, 3000)
        self.delete_host("TailLight")
        self.delete_topic("T100")
        self.delete_topic("T100_sub")
        self.delete_topic("T400")
        self.delete_topic("T400_sub")
        time.sleep(10)
        self.initial_setup()
        
        
    def run_script(self):
        
        self.reset()
        self.initial_setup()
        time.sleep(10)
        self.add_overloading_process()
        time.sleep(10)
        
        # start_time =time.time()
        # while time.time() - start_time < 300:
        brake_state_thread = threading.Thread(target=self.send_brake_state_ok)
        tail_light_state_thread = threading.Thread(target=self.send_tail_light_state_ok)
        tail_light_command_thread = threading.Thread(target=self.send_tail_light_command_ok)
        overloading_thread = threading.Thread(target=self.increase_overloading)
        brake_state_thread.start()
        tail_light_state_thread.start()
        tail_light_command_thread.start()
        overloading_thread.start()
        
        
        