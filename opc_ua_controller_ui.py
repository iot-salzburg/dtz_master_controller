#      _____         __        __                               ____                                        __
#     / ___/ ____ _ / /____   / /_   __  __ _____ ____ _       / __ \ ___   _____ ___   ____ _ _____ _____ / /_
#     \__ \ / __ `// //_  /  / __ \ / / / // ___// __ `/      / /_/ // _ \ / ___// _ \ / __ `// ___// ___// __ \
#    ___/ // /_/ // /  / /_ / /_/ // /_/ // /   / /_/ /      / _, _//  __/(__  )/  __// /_/ // /   / /__ / / / /
#   /____/ \__,_//_/  /___//_.___/ \__,_//_/    \__, /      /_/ |_| \___//____/ \___/ \__,_//_/    \___//_/ /_/
#                                              /____/
#   Salzburg Research ForschungsgesmbH
#   Armin Niedermueller

#   OPC UA Server on Master Cluster
#   The purpose of this OPCUA client is to call the provided methods of the ConveyorBelt and Robot  and read
#   their (state) variables


from datetime import datetime
from opcua import Client, ua, uamethod, Server
import threading, queue
import requests
import pytz
import time
import sys

sys.path.insert(0, "..")


################# GLOBAL VARIABLES #################

url_opcua_adapter = "192.168.48.81:1337"
url_panda_server = "opc.tcp://192.168.48.41:4840/freeopcua/server/"
url_pixtend_server = "opc.tcp://192.168.48.42:4840/freeopcua/server/"
url_fhs_server = "opc.tcp://192.168.10.138:4840"
url_pseudo_fhs_server = "opc.tcp://192.168.48.44:4840/freeopcua/server/"

desired_distance = 0.55  # distance in meters to drive the belt
belt_velocity = 0.05428  # velocity of the belt in m/s (5.5cm/s)
timebuffer = 3           # time buffer for the wait loops after method call. wifi istn that fast
storage = []             # our storage data as an array
global_new_val_available = False
global_demonstrator_busy = None
global_belt_moving = None
global_panda_obj = None
global_pixtend_obj = None


##################### METHODS ######################

def start_demo_core(shelf, available, cbelt_moving, panda_obj, pixtend_obj, demo_busy):


    if available == True and cbelt_moving == False:

        demo_busy = True
        # Methods
        pixtend_obj.call_method("2:MoveBelt", "left", 0.55)  # drive 55cm right
        panda_obj.call_method("2:MoveRobot", "SO", str(desired_shelf))

        # hier einfügen, prüfen auf variablen von panda ferttig und belt fertig

        conbelt.move_right_for(distance)
    elif direction == "left":
        conbelt.move_left_for(distance)
    return True

@uamethod
def start_demo(parent, shelf):
    print("Move Demo")
    move_thread = threading.Thread(name='move_demo_thread', target = start_demo_core, args = (shelf,
                                                                                              global_new_val_available,
                                                                                              global_belt_moving,
                                                                                              global_panda_obj,
                                                                                              global_pixtend_obj,
                                                                                              global_demonstrator_busy,))
    move_thread.daemon = True
    move_thread.start()
    return True




################ DATACHANGE HANDLER ################

class SubHandler(object):

    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """

    def __init__(self, shelf_fh_server_object, panda_moving_pandapc_object, belt_moving_pixtend_object, panda_object, pixtend_object):
        self.shelf_nr = shelf_fh_server_object.getvalue()
        self.panda_is_moving = panda_moving_pandapc_object.getvalue()
        self.belt_is_moving = belt_moving_pixtend_object.getvalue()
        self.panda_obj = panda_object
        self.belt_obj = pixtend_object


    def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val, data)

        # data = NewValueAvailable
        if data == "true" :
            # METHOD CALLS
            self.panda_obj.call_method("MoveRobot", "SO", self.shelf_nr)
            while self.panda_is_moving:
                time.sleep(0.1)

            self.belt_obj.call_method("2:MoveBelt", "left", 0.6)
            while self.belt_is_moving:
                time.sleep(0.1)


    def event_notification(self, event):
        print("Python: New event", event)



################################################# START #######################################################

if __name__ == "__main__":

    ################ CLIENT SETUP I ################

    client_panda = Client(url_panda_server)
    client_pixtend = Client(url_pixtend_server)
    client_fhs = Client(url_pseudo_fhs_server)
    # client = Client("opc.tcp://admin@localhost:4840/freeopcua/server/") #connect using a user


    try:
        ############# LOAD STORAGE DATA  #############

        # [1][2][3]
        # [4][5][6]
        # [7][8][9]

        with open("./dtz_storage", "r", encoding="utf-8") as inputfile:
            for line in inputfile:
                storage.append(line)


        ################ SERVER SETUP ################

        # setup server
        server = Server()
        url = "opc.tcp://0.0.0.0:4840/freeopcua/server"
        server.set_endpoint(url)
        # setup namespace
        uri = "https://github.com/iot-salzburg/dtz_master_controller"
        idx = server.register_namespace(uri)

        # get Objects node, this is where we should put our nodes
        objects = server.get_objects_node()

        # Add a parameter object to the address space
        master_object = objects.add_object(idx, "DTZMasterController")

        # Parameters - Addresspsace, Name, Initial Value
        server_time = master_object.add_variable(idx, "ServerTime", 0)
        global_demonstrator_busy = demonstrator_busy = master_object.add_variable(idx, "DemonstratorBusy", False)
        mover = master_object.add_method(idx, "MoveDemonstrator", move_demonstrator, [ua.VariantType.String, ua.VariantType.String], [ua.VariantType.Boolean])

        # Start the server
        server.start()

        print("OPC-UA - Master - Server started at {}".format(url))

        ###############  CLIENT SETUP II ###############

        # connect to servers
        client_panda.connect()
        client_pixtend.connect()
        client_fhs.connect()

        # Get root nodes
        root_panda = client_panda.get_root_node()
        root_pixtend = client_pixtend.get_root_node()
        root_fhs = client_fhs.get_root_node()


        ################ GET VARIABLES FROM SERVER ################

        # get our desired objects
        object_fhs = root_fhs.get_child(["0:Objects", "2:PLC"])
        object_panda = root_panda.get_child(["0:Objects", "2:PandaRobot"])
        object_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt"])

        # VALUES
        mover_panda = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:MoveRobot"])
        panda_state = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotState"])

        panda_moving = root_panda.get_child(["0:Objects", "2:Object1", "2:RobotMoving"])

        mover_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:MoveBelt"])
        conbelt_state = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltState"])
        conbelt_dist = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltDist"])

        belt_moving = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltMoving"])

        # get the control values from fh salzburg server
        desired_shelf = object_fhs.get_child(["2:ShelfNumber"])
        new_val_available = object_fhs.get_child(["2:NewValueAvailable"])
        task_running = object_fhs.get_child(["2:TaskRunning"])


        ################ STORAGE OUTPUT ################

        print("---------------------------")
        local_shelf = desired_shelf.get_value()-1   # Shelf 1-9 to array 0-8
        print("Desired shelf on FHS Server is " + str(local_shelf+1))   # print shelf 1-9

        print("---------------------------")
        print("Storage containing " + str(len(storage)) + " fields")
        i = 0
        while i < len(storage):
            print("[" + str(i+1) + "]: " + str(storage[i]), end="")
            i = i + 1
        print("\n---------------------------")

        if str(int(storage[local_shelf])) == "1":   # Shelf 0-8
            print("Desired shelf [" + str(local_shelf+1) + "] is not empty")

        else:
            print("Desired shelf [" + str(local_shelf+1) + "] is empty")
        print("---------------------------")


        ###### SUBSCRIBE TO SERVER DATA CHANGES #######

        handler = SubHandler(local_shelf+1, panda_moving, belt_moving, object_panda, object_pixtend)
        sub = client_fhs.create_subscription(500, handler)
        handle = sub.subscribe_data_change(new_val_available)
        time.sleep(0.1)


        ########################### RUNNNING LOOP ##############################
        print("Starting and running...")
        task_running.set_value(True)

        while True:

            # Send data to FH Salzburg Server when panda robot is moving
            if demonstrator_busy:
                print("Demonstrator is busy")
                task_running.set_value(True)
            elif not demonstrator_busy:
                print("Demonstrator is not busy")
                task_running.set_value(False)


            # Sending changed states to kafka stack
            tm = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
            r1 = requests.post(url_opcua_adapter,data={'id': 'pandapc.panda_state', 'timestamp': tm, 'panda_state': panda_state})
            r2 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_state', 'timestamp': tm, 'conbelt_state': conbelt_state})
            r3 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_dist', 'timestamp': tm, 'conbelt_dist': conbelt_dist})

            time.sleep(0.3)



    except KeyboardInterrupt:
        print("\nClient stopped...CTRL+C pressed")
    except requests.exceptions.ConnectionError:
        print("error connecting...")
    finally:
        print("\nClient stopped")
        client_pixtend.disconnect()
        client_panda.disconnect()
        client_fhs.disconnect()

        ############# SAVE STORAGE DATA  #############

        # [1][2][3]
        # [4][5][6]
        # [7][8][9]

        with open("./dtz_storage", "w", encoding="utf-8") as outputfile:
            for i in storage:
                outputfile.write(str(i))
