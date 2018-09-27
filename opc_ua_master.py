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
import threading
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
storage = []             # our storage data as an array
global_new_val_available = None
global_demonstrator_busy = None
global_belt_moving = None
global_panda_moving = None
global_object_panda = None
global_object_pixtend = None
global_desired_shelf = None


##################### METHODS ######################

def start_demo_core(movement):

        global_object_panda.call_method("2:MoveRobot", movement, str(global_desired_shelf.get_value()))

        while global_panda_moving:
            time.sleep(0.2)
            global_object_pixtend.call_method("2:MoveBelt", "left", 0.55)  # drive 55cm right

        while global_belt_moving.get_value():
            time.sleep(0.2)

        return True


@uamethod
def start_demo(parent, movement, shelf):

    global storage

    if storage[shelf] is "0":
        return "Shelf empty - error!"

    elif global_demonstrator_busy.get_value() is False:

        global_demonstrator_busy.set_value(True)
        move_thread = threading.Thread(name='move_demo_thread', target=start_demo_core, args=(movement, ))
        move_thread.daemon = True
        move_thread.start()
        storage[shelf] = "0"    # make shelf empty
        return "Demonstrator is busy - error!"

    else:

        return "Shelf not empty - Demonstrator started!"


################ DATACHANGE HANDLER ################

class SubHandler(object):

    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """

    def __init__(self, shelf_nr, panda_moving, belt_moving, panda_object, pixtend_object):
        self.shelf_nr = shelf_nr
        self.panda_is_moving = panda_moving
        self.belt_is_moving = belt_moving
        self.panda_obj = panda_object
        self.belt_obj = pixtend_object
        self.storage = []



    def move_robot_core(self, movement, shelf_nr):

        self.panda_obj.call_method("2:MoveRobot", movement, str(shelf_nr))

        time.sleep(3)
        while global_panda_moving.get_value():
            print("while global panda moving")
            time.sleep(0.1)

        return True


    def move_belt_core(self, movement, distance):

        self.belt_obj.call_method("2:MoveBelt", movement, distance)

        while global_belt_moving:
            time.sleep(0.1)

        return True


    def datachange_notification(self, node, val, data):
        print("Python: New data change event on fhs server: NewValAvailable=", val)


        # data = NewValueAvailable
        if val is True and global_demonstrator_busy is False:
            print("global_demonstrator_busy:" + str(val))

            ############# LOAD STORAGE DATA  #############
            # [1][2][3]
            # [4][5][6]
            # [7][8][9]
            with open("./dtz_storage", "r", encoding="utf-8") as in_file:
                for in_line in in_file:
                    self.storage.append(in_line)

            # GET SOME VALUES FROM FHS SERVER
            handler_client_fhs = Client(url_pseudo_fhs_server)
            handler_client_fhs.connect()
            handler_root_fhs = handler_client_fhs.get_root_node()
            handler_object_fhs = handler_root_fhs.get_child(["0:Objects", "2:PLC"])
            handler_desired_shelf = handler_object_fhs.get_child(["2:ShelfNumber"])

            # IS THE STORAGE EMPTY?
            if self.storage[handler_desired_shelf.get_value()-1] is "0":
                return "Shelf empty - error!"
            else:
                # METHOD CALLS
                move_panda_thread = threading.Thread(name='move_panda_thread', target=self.move_robot_core, args=("SO", handler_desired_shelf.get_value(),))
                move_panda_thread.daemon = True
                move_panda_thread.start()
                move_panda_thread.join()

                move_belt_thread = threading.Thread(name='move_belt_thread', target=self.move_belt_core, args=("left", 0.55,))
                move_belt_thread.daemon = True
                move_belt_thread.start()
                self.storage[handler_desired_shelf.get_value()-1] = "0"

                handler_client_fhs.disconnect()

                ############# SAVE STORAGE DATA  #############
                # [1][2][3]
                # [4][5][6]
                # [7][8][9]
                with open("./dtz_storage", "w", encoding="utf-8") as out_file:
                    for out_line in self.storage:
                        out_file.write(str(out_line))

                return "Shelf not empty - successful!"


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
        with open("./dtz_storage", "r", encoding="utf-8") as input_file:
            for line in input_file:
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
        global_demonstrator_busy = master_object.add_variable(idx, "DemonstratorBusy", False)
        mover = master_object.add_method(idx, "MoveDemonstrator", start_demo, [ua.VariantType.String, ua.VariantType.Int64], [ua.VariantType.Boolean])

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
        global_object_panda = root_panda.get_child(["0:Objects", "2:PandaRobot"])
        global_object_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt"])

        # VALUES
        mover_panda = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:MoveRobot"])
        panda_state = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotState"])
        global_panda_moving = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotMoving"])

        mover_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:MoveBelt"])
        conbelt_state = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltState"])
        conbelt_dist = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltDist"])

        global_belt_moving = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltMoving"])

        # get the control values from fh salzburg server
        global_desired_shelf = object_fhs.get_child(["2:ShelfNumber"])
        global_new_val_available = object_fhs.get_child(["2:NewValueAvailable"])
        task_running = object_fhs.get_child(["2:TaskRunning"])
        desired_shelf = object_fhs.get_child(["2:ShelfNumber"])


        ################ STORAGE OUTPUT ################

        print("---------------------------")
        print("shelfie " + str(desired_shelf.get_value()))
        local_shelf = global_desired_shelf.get_value()-1   # Shelf 1-9 to array 0-8
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

        demo_handler = SubHandler(str(local_shelf+1), global_panda_moving.get_value(), global_belt_moving.get_value(),global_object_panda, global_object_pixtend)
        sub = client_fhs.create_subscription(500, demo_handler)
        demo_handle = sub.subscribe_data_change(global_new_val_available)
        time.sleep(0.1)


        # Sending changed states to kafka stack
        tm = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
        # r1 = requests.post(url_opcua_adapter,data={'id': 'pandapc.panda_state', 'timestamp': tm, 'panda_state': panda_state})
        # r2 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_state', 'timestamp': tm, 'conbelt_state': conbelt_state})
        # r3 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_dist', 'timestamp': tm, 'conbelt_dist': conbelt_dist})


        ########################### RUNNNING LOOP ##############################
        print("Starting and running...")
        task_running.set_value(True)
        global_demonstrator_busy.set_value(True)

        while True:

            if global_panda_moving.get_value() or global_belt_moving.get_value():
                task_running.set_value(True)
            else:
                task_running.set_value(False)

            time.sleep(0.4)



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

        with open("./dtz_storage", "w", encoding="utf-8") as output_file:
            for line in storage:
                output_file.write(str(line))
