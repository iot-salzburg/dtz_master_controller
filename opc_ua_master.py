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
import logging
import traceback


# create logger
logger = logging.getLogger('dtz_master_controller')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

# 'application' code
logger.debug('debug message')
logger.info('info message')
logger.warning('warn message')
logger.error('error message')
logger.critical('critical message')

sys.path.insert(0, "..")


################# GLOBAL VARIABLES #################
global_url_opcua_adapter = "192.168.48.81:1337"
global_url_panda_server = "opc.tcp://192.168.48.41:4840/freeopcua/server/"
global_url_pixtend_server = "opc.tcp://192.168.48.42:4840/freeopcua/server/"
global_url_fhs_server = "opc.tcp://192.168.10.102:4840"
global_url_pseudo_fhs_server = "opc.tcp://192.168.48.44:4840/freeopcua/server/"

global_desired_distance = 0.55  # distance in meters to drive the belt
global_belt_velocity = 0.05428  # velocity of the belt in m/s (5.5cm/s)
global_storage = []             # our storage data as an array

global_new_val_available = None
global_demonstrator_busy = None
global_belt_moving = None
global_panda_moving = None
global_object_panda = None
global_object_pixtend = None
global_desired_shelf = None
global_belt_moved = None
global_panda_moved = None


######################################## CALL BY METHOD CALL ON MASTER #################################################


def move_robot_core(self, movement, shelf_nr):
    global global_panda_moving
    global global_object_panda

    # self.panda_obj.call_method("2:MoveRobotLibfranka", movement, str(shelf_nr))
    global_object_panda.call_method("2:MoveRobotRos", movement, str(shelf_nr))
    logger.debug("move robot to shelf %s", shelf_nr)
    time.sleep(3)
    self.panda_moved = False
    logger.debug("robot core")
    mytime = 0
    while not global_panda_moving.get_value():
        time.sleep(0.1)
        mytime += 0.1
        # panda does not react
        if mytime >= 6:
            logger.debug("waited for: %s seconds without detecting panda moving", mytime)
            self.panda_moved = False
            return False

    logger.debug("out")

    logger.debug("panda moving: " + str(global_panda_moving.get_value()))
    while global_panda_moving.get_value():
        time.sleep(0.1)
        logger.debug("panda move finished")
    self.panda_moved = True
    return True

@uamethod
def move_belt_core(self, movement, shelf):
    global global_belt_moved
    global_belt_moved = False
    global global_object_pixtend
    global global_belt_moving

    global_object_pixtend.call_method("2:MoveBelt", "left", 0.55)


    wait_time = 0
    while not global_belt_moving.get_value():
        time.sleep(0.1)
        wait_time += 0.1
        logger.debug("time: %s", wait_time)
        # panda does not react
        if wait_time >= 3:
            global_belt_moved = False
            return False

    global_belt_moved = True
    return True


def start_demo(parent, movement, shelf):
    global global_demonstrator_busy
    global global_desired_shelf
    global global_panda_moving
    global global_belt_moving
    global global_panda_moved
    global global_belt_moved
    exit = "None"

    logger.debug("starting demo by method call")

    if global_demonstrator_busy.get_value() is False:
        logger.debug("global_demonstrator_busy: " + str(global_demonstrator_busy))

        ############# LOAD STORAGE DATA  #############
        # [1][2][3]
        # [4][5][6]
        # [7][8][9]
        with open("./dtz_storage", "r", encoding="utf-8") as in_file:
            for in_line in in_file:
                global_storage.append(in_line)

        # IS THE STORAGE EMPTY?


        if global_storage[shelf-1] is "0":
            exit = "Shelf empty - error!"
        else:
            global_demonstrator_busy.set_value(True)

            # METHOD CALLS
            move_panda_thread = threading.Thread(name='move_panda_thread', target=move_robot_core,
                                                 args=("SO", global_desired_shelf.get_value(),))
            move_panda_thread.daemon = True
            move_panda_thread.start()
            move_panda_thread.join()

            # move_panda_thread.wait()

            logger.debug("p_moved %s", global_panda_moved)
            if global_panda_moved is True:
                move_belt_thread = threading.Thread(name='move_belt_thread', target=move_belt_core,
                                                    args=("left", 0.55,))
                move_belt_thread.daemon = True
                move_belt_thread.start()
                global_storage[global_desired_shelf.get_value() - 1] = "0"
                move_belt_thread.join()
                if not global_belt_moved:
                    logger.debug("Error - Belt not moved")
                    exit = "Error - Belt not moved"
            else:
                logger.debug("Error - Panda not moved")
                exit = "Error - Panda not moved"

            ############# SAVE STORAGE DATA  #############
            # [1][2][3]
            # [4][5][6]
            # [7][8][9]
            with open("./dtz_storage", "w", encoding="utf-8") as out_file:
                for out_line in global_storage:
                    out_file.write(str(out_line))

            exit = "Shelf not empty - successful!"

    logger.debug("exiting datachange_notification. return message: %s", exit)
    return exit


######################################## CALL BY DATACHANGE ON FH SERVER ###############################################

class SubHandler(object):

    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """

    def __init__(self):
        self.panda_moved = False
        self.belt_moved = False


    def move_robot_core(self, movement, shelf_nr):
        global global_panda_moving
        global global_object_panda

        #self.panda_obj.call_method("2:MoveRobotLibfranka", movement, str(shelf_nr))
        global_object_panda.call_method("2:MoveRobotRos", movement, str(shelf_nr))
        logger.debug("move robot to shelf %s",shelf_nr)
        time.sleep(3)
        self.panda_moved = False
        logger.debug("robot core")
        mytime = 0
        while not global_panda_moving.get_value():
            time.sleep(0.1)
            mytime += 0.1
            # panda does not react
            if mytime >= 6:
                logger.debug("waited for: %s seconds without detecting panda moving", mytime)
                self.panda_moved = False
                return False

        logger.debug("out")

        logger.debug("panda moving: " + str(global_panda_moving.get_value()))
        while global_panda_moving.get_value():
            time.sleep(0.1)
        logger.debug("panda move finished")
        self.panda_moved = True
        return True


    def move_belt_core(self, movement, distance):
        global global_object_pixtend
        global global_belt_moving

        global_object_pixtend.call_method("2:MoveBelt", movement, distance)
        self.belt_moved = False

        mytime = 0
        while not global_belt_moving.get_value():
            time.sleep(0.1)
            mytime += 0.1
            logger.debug("time: %s", mytime)
            # panda does not react
            if mytime >= 3:
                self.belt_moved = False
                return False

        self.belt_moved = True
        return True


    def datachange_notification(self, node, val, data):
        global global_demonstrator_busy
        global global_desired_shelf
        global global_panda_moving
        global global_belt_moving

        logger.debug("Python: New data change event on fhs server: NewValAvailable=%s", val)


        # data = NewValueAvailable
        exit = "NewValAvailable is {}, demonstratorBusy is {}".format(val, global_demonstrator_busy)

        if val is True and global_demonstrator_busy.get_value() is False :
            logger.debug("global_demonstrator_busy: " + str(global_demonstrator_busy) + ". NewValAvailable: " + str(val))

            ############# LOAD STORAGE DATA  #############
            # [1][2][3]
            # [4][5][6]
            # [7][8][9]
            with open("./dtz_storage", "r", encoding="utf-8") as in_file:
                for in_line in in_file:
                    global_storage.append(in_line)


            # IS THE STORAGE EMPTY?

            global_storage[global_desired_shelf-1] = 1  ######################## <<<<<<<<<<<<<--- OVERRIDES NEXT LINE

            if global_storage[global_desired_shelf-1] is "0":
                exit = "Shelf empty - error!"
            else:
                global_demonstrator_busy.set_value(True)


                # METHOD CALLS
                move_panda_thread = threading.Thread(name='move_panda_thread', target=self.move_robot_core, args=("SO", global_desired_shelf.get_value(), ))
                move_panda_thread.daemon = True
                move_panda_thread.start()
                move_panda_thread.join()

               # move_panda_thread.wait()

                logger.debug("p_moved %s", self.panda_moved)
                if self.panda_moved is True:
                    move_belt_thread = threading.Thread(name='move_belt_thread', target=self.move_belt_core, args=("left", 0.55,))
                    move_belt_thread.daemon = True
                    move_belt_thread.start()
                    global_storage[global_desired_shelf.get_value()-1] = "0"
                    move_belt_thread.join()
                    if not self.belt_moved:
                        logger.debug("Error - Belt not moved")
                        exit = "Error - Belt not moved"
                else:
                    logger.debug("Error - Panda not moved")
                    exit = "Error - Panda not moved"


                ############# SAVE STORAGE DATA  #############
                # [1][2][3]
                # [4][5][6]
                # [7][8][9]
                with open("./dtz_storage", "w", encoding="utf-8") as out_file:
                    for out_line in global_storage:
                        out_file.write(str(out_line))

                exit = "Shelf not empty - successful!"

        logger.debug("exiting datachange_notification. return message: %s", exit)
        return exit


    def event_notification(self, event):
        logger.debug("Python: New event", event)



################################################# START #######################################################

if __name__ == "__main__":


    reconnect_counter = 0

    ################ CLIENT SETUP I ################

    client_panda = Client(global_url_panda_server)
    client_pixtend = Client(global_url_pixtend_server)
    client_fhs = Client(global_url_fhs_server)
    # client = Client("opc.tcp://admin@localhost:4840/freeopcua/server/") #connect using a user

    ################ SERVER SETUP ################

    # setup server
    server = Server()
    url = "opc.tcp://0.0.0.0:4840/freeopcua/server"
    server.set_endpoint(url)
    # setup namespace
    uri = "urn:freeopcua"
    idx = server.register_namespace(uri)

    # get Objects node, this is where we should put our nodes
    objects = server.get_objects_node()

    # Add a parameter object to the address space
    master_object = objects.add_object(idx, "DTZMasterController")

    # Parameters - Addresspsace, Name, Initial Value
    server_time = master_object.add_variable(idx, "ServerTime", 0)
    global_demonstrator_busy = master_object.add_variable(idx, "DemonstratorBusy", False)
    global_demonstrator_busy.set_writable()
    mover = master_object.add_method(idx, "MoveDemonstrator", start_demo, [ua.VariantType.String, ua.VariantType.Int64],
                                     [ua.VariantType.Boolean])

    while True:

        try:

            # Start the server
            server.start()

            logger.debug("OPC-UA - Master - Server started at {}".format(url))


            ###############  CLIENT SETUP II ###############

            # connect to servers
            logger.debug("connecting to panda server")
            client_panda.connect()
            logger.debug("connecting to pixtend server")
            client_pixtend.connect()
            logger.debug("connecting to fhs server")
            client_fhs.connect()

            # Get root nodes
            root_panda = client_panda.get_root_node()
            root_pixtend = client_pixtend.get_root_node()
            root_fhs = client_fhs.get_root_node()

            # Connection successful?
            reconnect_counter = 0

            ################ GET VARIABLES FROM SERVER ################

            # get our desired objects
            #object_fhs = root_fhs.get_child(["0:Objects", "2:PLC"])
            global_object_panda = root_panda.get_child(["0:Objects", "2:PandaRobot"])
            global_object_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt"])

            # VALUES
            mover_panda_ros = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:MoveRobotRos"])
            mover_panda_libfranka = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:MoveRobotLibfranka"])
            panda_state = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotState"])
            global_panda_moving = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotMoving"])

            mover_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:MoveBelt"])
            conbelt_state = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltState"])
            conbelt_dist = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltDist"])

            global_belt_moving = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltMoving"])

            # get the control values from fh salzburg server
            global_desired_shelf = client_fhs.get_node("ns=6;s=::AsGlobalPV:ShelfNumber")

            global_new_val_available = client_fhs.get_node("ns=6;s=::AsGlobalPV:NewValAvailable")             ###### ORIGINAL!!!
            #global_new_val_available = client_fhs.get_node("ns=6;s=::AsGlobalPV:StartRobot")

            task_running = client_fhs.get_node("ns=6;s=::AsGlobalPV:TaskRunning")


            ################ STORAGE OUTPUT ################

            logger.debug("---------------------------")
            logger.debug("shelfie " + str(global_desired_shelf.get_value()))
            local_shelf = global_desired_shelf.get_value()-1   # Shelf 1-9 to array 0-8
            logger.debug("Desired shelf on FHS Server is " + str(local_shelf+1))   # print shelf 1-9

            logger.debug("---------------------------")
            logger.debug("Storage containing " + str(len(global_storage)) + " fields")
            i = 0
            while i < len(global_storage):
                logger.debug("[" + str(i+1) + "]: " + str(global_storage[i]))
                i = i + 1

            logger.debug("\n---------------------------")

            if str(int(global_storage[local_shelf])) == "1":   # Shelf 0-8
                logger.debug("Desired shelf [" + str(local_shelf+1) + "] is not empty")
            else:
                logger.debug("Desired shelf [" + str(local_shelf+1) + "] is empty")
            logger.debug("---------------------------")


            ###### SUBSCRIBE TO SERVER DATA CHANGES #######

            demo_handler = SubHandler( )
            sub = client_fhs.create_subscription(500, demo_handler)
            demo_handle = sub.subscribe_data_change(global_new_val_available)
            time.sleep(0.1)


            # Sending changed states to kafka stack
            tm = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
            # r1 = requests.post(url_opcua_adapter,data={'id': 'pandapc.panda_state', 'timestamp': tm, 'panda_state': panda_state})
            # r2 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_state', 'timestamp': tm, 'conbelt_state': conbelt_state})
            # r3 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_dist', 'timestamp': tm, 'conbelt_dist': conbelt_dist})


            ########################### RUNNNING LOOP ##############################
            logger.debug("Starting and running...")

            #task_running.set_value(True)
            global_demonstrator_busy.set_value(True)

            while True:
                #logger.debug("panda moving: " + str(global_panda_moving.get_value()) + ". belt_moving: " + str(global_belt_moving.get_value()))

                #logger.debug("global_panda_moving: " + str(global_panda_moving.get_value()) + ". global_belt_moving: " + str(global_belt_moving.get_value()))

                if global_panda_moving.get_value() or global_belt_moving.get_value():
                    global_demonstrator_busy.set_value(True)
                else:
                    global_demonstrator_busy.set_value(False)


                #logger.debug("global_demonstrator busy: " + str(global_demonstrator_busy.get_value()))

                time.sleep(0.5)


        except KeyboardInterrupt:
            logger.debug("\nCTRL+C pressed")
            server.stop()
            logger.debug("\nClients disconnected and Server stopped")
            break
        except requests.exceptions.ConnectionError:
            logger.debug("error connecting...")
        except Exception as e:
            try:
                client_pixtend.disconnect()
            except:
                pass
            try:
                client_panda.disconnect()
            except:
                pass
            try:
                client_fhs.disconnect()
            except:
                pass

            # try connecting again
            time.sleep(2**reconnect_counter)
            reconnect_counter += 1
            logger.debug("Error while connecting to servers - " + str(e) + " trying again in " + str(2**reconnect_counter) + " seconds.")
            logger.debug(traceback.format_exc())
            continue
        finally:
            server.stop()
            logger.debug("\nClients disconnected and Server stopped")

    # after while loop
    client_fhs.disconnect()
    client_panda.disconnect()
    client_pixtend.disconnect()


