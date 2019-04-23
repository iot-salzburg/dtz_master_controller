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
logger.warn('warn message')
logger.error('error message')
logger.critical('critical message')

sys.path.insert(0, "..")

################# GLOBAL VARIABLES #################

global_url_opcua_adapter = "192.168.48.81:1337"
global_url_panda_server = "opc.tcp://192.168.48.41:4840/freeopcua/server/"
global_url_pixtend_server = "opc.tcp://192.168.48.42:4840/freeopcua/server/"
global_url_fhs_server = "opc.tcp://192.168.10.102:4840"
global_url_pseudo_fhs_server = "opc.tcp://192.168.48.44:4840/freeopcua/server/"

desired_distance = 0.55  # distance in meters to drive the belt
belt_velocity = 0.05428  # velocity of the belt in m/s (5.5cm/s)
storage = []  # our storage data as an array
global_new_val_available = None
global_demonstrator_busy = None
global_belt_moving = None
global_panda_moving = None
global_object_panda = None
global_object_pixtend = None
global_desired_shelf = None
subhandler_already_created = False


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
        self.handler_panda_moving = None
        self.handler_belt_moving = None
        self.handler_demonstrator_busy = None
        self.panda_moved = False
        self.belt_moved = False

    def move_robot_core(self, movement, shelf_nr):

        # self.panda_obj.call_method("2:MoveRobotLibfranka", movement, str(shelf_nr))
        self.panda_obj.call_method("2:MoveRobotRos", movement, str(shelf_nr))
        logger.debug("move robot to shelf %s", shelf_nr)
        time.sleep(3)
        self.panda_moved = False
        logger.debug("robot core")
        mytime = 0
        while not self.handler_panda_moving.get_value():
            time.sleep(0.1)
            mytime += 0.1
            # panda does not react
            if mytime >= 6:
                logger.debug("waited for: %.2s seconds without detecting panda moving", mytime)
                self.panda_moved = False
                return False


        logger.debug("panda moving: " + str(self.handler_panda_moving.get_value()))
        while self.handler_panda_moving.get_value():
            time.sleep(0.1)
        logger.debug("panda move finished")
        self.panda_moved = True
        return True

    def move_belt_core(self, movement, distance):

        self.belt_obj.call_method("2:MoveBelt", movement, distance)
        self.belt_moved = False

        mytime = 0
        while not self.handler_belt_moving.get_value():
            time.sleep(0.1)
            mytime += 0.1
            # logger.debug("time: %.2s", mytime)
            # panda does not react
            if mytime >= 3:
                self.belt_moved = False
                return False

        self.belt_moved = True
        return True

    def datachange_notification(self, node, val, data):
        try:

            logger.debug("handler: New data change event on fhs server: NewValAvailable=%s", val)

            # GET SOME VALUES FROM THIS SERVER
            logger.debug("handler: connecting to DTZ master Server")
            this_client = Client("opc.tcp://0.0.0.0:4840/freeopcua/server")
            this_client.connect()
            this_client_root = this_client.get_root_node()
            self.demonstrator_busy = this_client_root.get_child(
                ["0:Objects", "2:DTZMasterController", "2:DemonstratorBusy"])

            # GET SOME VALUES FROM FHS SERVER
            logger.debug("handler: connecting to FHS Server")
            # handler_client_fhs = Client(global_url_pseudo_fhs_server)                                                  # Testing with pseudo FH server
            handler_client_fhs = Client(global_url_fhs_server)  # Original
            handler_client_fhs.connect()
            handler_root_fhs = handler_client_fhs.get_root_node()
            # handler_desired_shelf = handler_client_fhs.get_node("ns=2;i=3")                                            # Testing with pseudo FH server
            handler_desired_shelf = handler_client_fhs.get_node("ns=6;s=::AsGlobalPV:ShelfNumber")  # Original

            # GET VALUES FROM PANDA SERVER
            logger.debug("handler: connecting to Panda Server")
            handler_client_panda = Client(global_url_panda_server)
            handler_client_panda.connect()
            handler_root_panda = handler_client_panda.get_root_node()
            self.handler_panda_moving = handler_root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotMoving"])

            # GET VALUES FROM PIXTEND SERVER
            logger.debug("handler: connecting to Pixtend Server")
            handler_client_pixtend = Client(global_url_pixtend_server)
            handler_client_pixtend.connect()
            handler_root_pixtend = handler_client_pixtend.get_root_node()
            self.handler_belt_moving = handler_root_pixtend.get_child(
                ["0:Objects", "2:ConveyorBelt", "2:ConBeltMoving"])

            # data = NewValueAvailable
            demoBusy = self.demonstrator_busy.get_value()
            exit = "NewValAvailable is {}, demonstratorBusy is {}".format(val, demoBusy)

            if val is True and demoBusy is False:
                logger.debug("handler: global_demonstrator_busy: " + str(demoBusy) + ". NewValAvailable: " + str(
                    val) + ". ShelfNumber: " + str(handler_desired_shelf.get_value()) + ".")

                ############# LOAD STORAGE DATA  #############
                # [1][2][3]
                # [4][5][6]
                # [7][8][9]

                with open("./dtz_storage", "r", encoding="utf-8") as in_file:
                    for in_line in in_file:
                        self.storage.append(in_line)

                # IS THE STORAGE EMPTY?
                self.storage[handler_desired_shelf.get_value() - 1] = "1"

                #if self.storage[handler_desired_shelf.get_value() - 1] is "0":   # commented because of problems with demonstrator
                if False:
                    exit = "Shelf empty - error!"
                else:
                    self.demonstrator_busy.set_value(True)

                    # METHOD CALLS
                    move_panda_thread = threading.Thread(name='move_panda_thread', target=self.move_robot_core,
                                                         args=("SO", handler_desired_shelf.get_value(),))
                    move_panda_thread.daemon = True
                    move_panda_thread.start()
                    move_panda_thread.join()

                    # logger.debug("p_moved %s", self.panda_moved)
                    if self.panda_moved is True:
                        move_belt_thread = threading.Thread(name='move_belt_thread', target=self.move_belt_core,
                                                            args=("left", 0.55,))
                        move_belt_thread.daemon = True
                        move_belt_thread.start()
                        self.storage[handler_desired_shelf.get_value() - 1] = "0"
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
                        for out_line in self.storage:
                            out_file.write(str(out_line))

            logger.debug("handler: disconnect from fhs server")
            handler_client_fhs.disconnect()
            logger.debug("handler: disconnect from panda server")
            handler_client_panda.disconnect()
            logger.debug("handler: disconnect from pixtend server")
            handler_client_pixtend.disconnect()
            this_client.disconnect()

        except Exception as e:
            logger.debug("handler: Catched Exception: " + str(e))
            try:
                logger.debug("handler: trying to disconnect from pixtend server")
                handler_client_pixtend.disconnect()
            except:
                logger.debug("handler: pixtend server was disconnected")
                pass
            try:
                logger.debug("handler: trying to disconnect from panda server")
                handler_client_panda.disconnect()
            except:
                logger.debug("handler: panda server was disconnected")
                pass
            try:
                logger.debug("handler: trying to disconnect from fhs server")
                handler_client_fhs.disconnect()
            except:
                logger.debug("handler: fhs server was disconnected")
                pass
            return "handler: Error: " + str(e)

        logger.debug("handler: exiting datachange_notification. return message: %s", exit)
        return exit

    def event_notification(self, event):
        logger.debug("handler: New event", event)



######################### CONNECTION THREADS #################

###############  THREAD - CONNECT TO FH SERVER ###############
# FH Connection in separate thread
def threaded_fh_connection():
    # reconnection counter
    fh_counter = 1

    global global_desired_shelf
    global global_panda_moving
    global global_belt_moving
    global global_object_panda
    global global_object_pixtend
    global global_new_val_available
    global subhandler_already_created
    temp = None
    temp_node = None
    sub = None


    while True:
        try:
            # connect to fhs server
            logger.debug("threaded: connecting to fhs server")
            client_fhs.connect()
            client_fhs_2.connect()
            logger.debug("threaded: successful connected to fhs")

            # Get root nodes
            root_fhs = client_fhs.get_root_node()

            ########## GET VARIABLES FROM SERVER ##########
            # get our desired objects
            # object_fhs = root_fhs.get_child(["0:Objects", "2:PLC"])

            # VALUES
            # get the control values from fh salzburg server
            global_desired_shelf = client_fhs.get_node("ns=6;s=::AsGlobalPV:ShelfNumber")  # Original
            # global_desired_shelf = client_fhs.get_node("ns=2;i=3")                                                     # Testing with pseudo FH server
            local_shelf = global_desired_shelf.get_value() - 1  # Shelf 1-9 to array 0-8

            temp_node = client_fhs_2.get_node("ns=6;s=::AsGlobalPV:ShelfNumber")  # For the 2nd Client in the while loop - otherwise exeption when datachange subscription routine is active and get.value() in while loop is called


            global_new_val_available = client_fhs.get_node("ns=6;s=::AsGlobalPV:NewValAvailable")  # Original
            # global_new_val_available = client_fhs.get_node("ns=2;i=4")                                                 #Testing with pseudo FH server
            task_running = client_fhs.get_node("ns=6;s=::AsGlobalPV:TaskRunning")


            ###### SUBSCRIBE TO SERVER DATA CHANGE ON FH SERVER #######
            demo_handler = SubHandler(str(local_shelf + 1), global_panda_moving.get_value(),
                                    global_belt_moving.get_value(), global_object_panda, global_object_pixtend)

            logger.debug("threaded: create subscription handle fh server")
            sub = client_fhs.create_subscription(500, demo_handler)
            demo_handle = sub.subscribe_data_change(global_new_val_available)

            # everything went fine? then wait
            while True:
                temp = temp_node.get_value() - 1  # Just to see if connection is still alive
                time.sleep(1)


        except Exception as e:
            logger.debug("threaded: Catched Exception: " + str(e))
            try:
                logger.debug("threaded: trying to disconnect from fh server")
                client_fhs.disconnect()
                client_fhs_2.disconnect()
                logger.debug("threaded: delete subscription handle fh server")
                sub.unsubscribe(demo_handle)
                sub.delete()
                demo_handle = None
                demo_handler = None
            except Exception as e:
                logger.debug("threaded: Catched Exception: " + str(e))

        logger.debug("threaded: no connection to fh server - trying again in " + str(fh_counter) + " seconds.")
        fh_counter = fh_counter * 2
        if fh_counter > 17:
            fh_counter = 1
        time.sleep(fh_counter)




###############  THREAD - CONNECT TO PANDA SERVER ###############
# Panda Connection in separate thread
def threaded_panda_connection():

    # reconnection counter
    panda_counter = 1

    global global_panda_moving
    global global_object_panda
    temp = None

    while True:
        try:
            logger.debug("threaded: connecting to panda server")
            client_panda.connect()
            logger.debug("threaded: successful connected to panda")

            # Get root nodes
            root_panda = client_panda.get_root_node()

            ########## GET VARIABLES FROM SERVER ##########
            # get our desired objects
            global_object_panda = root_panda.get_child(["0:Objects", "2:PandaRobot"])

            # VALUES
            mover_panda_ros = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:MoveRobotRos"])
            mover_panda_libfranka = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:MoveRobotLibfranka"])
            panda_state = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotState"])
            global_panda_moving = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotMoving"])

            # everything went fine? then wait
            while True:
                temp = global_panda_moving.get_value()
                time.sleep(0.5)

            try:
                client_panda.disconnect()
            except:
                pass

        except Exception as e:
            try:
                logger.debug("threaded: Catched Exception: " + str(e))
                logger.debug("threaded: trying to disconnect from panda server")
                client_panda.disconnect()
            except:
                pass

        logger.debug("threaded: no connection to panda server - trying again in " + str(panda_counter) + " seconds.")
        panda_counter = panda_counter * 2
        if panda_counter > 17:
            panda_counter = 1
        time.sleep(panda_counter)



###############  THREAD - CONNECT TO PIXTEND SERVER ###############
# Pixtend Connection in separate thread
def threaded_pixtend_connection():

    # reconnection counter
    pixtend_counter = 1

    global global_belt_moving
    global global_object_pixtend
    global global_belt_moving

    while True:
        try:
            logger.debug("threaded: connecting to pixtend server")
            client_pixtend.connect()
            logger.debug("threaded: successful connected to pixtend")

            # Get root nodes
            root_pixtend = client_pixtend.get_root_node()

            ########## GET VARIABLES FROM SERVER ##########
            # get our desired objects
            global_object_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt"])

            # VALUES
            conbelt_state = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltState"])
            conbelt_dist = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltDist"])
            busy_light = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:SwitchBusyLight"])
            global_belt_moving = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltMoving"])


            # everything went fine? then wait
            while True:
                temp = global_belt_moving.get_value()
                time.sleep(0.5)

        except Exception as e:
            logger.debug("threaded: Catched Exception: " + str(e))
            try:
                logger.debug("threaded: trying to disconnect from pixtend server")
                client_pixtend.disconnect()
            except:
                pass

        logger.debug("threaded: no connection to pixtend server - trying again in " + str(pixtend_counter) + " seconds.")
        pixtend_counter = pixtend_counter * 2
        if pixtend_counter > 17:
            pixtend_counter = 1
        time.sleep(pixtend_counter)



##################### METHODS ######################

def start_demo_core(movement, shelf):
    logger.debug("in start demo core")
    # global_object_panda.call_method("2:MoveRobotLibfranka", movement, str(global_desired_shelf.get_value()))
    global_object_panda.call_method("2:MoveRobotRos", movement, str(shelf))

    # wait for robot to even react otherwise the code would jump instantly to the end
    while not global_panda_moving.get_value():
        time.sleep(0.2)

    while global_panda_moving.get_value():
        time.sleep(0.2)

    global_object_pixtend.call_method("2:MoveBelt", "left", 0.55)  # drive 55cm right

    # wait for belt to even react otherwise the code would jump instantly to the end
    while not global_belt_moving.get_value:
        time.sleep(0.2)

    while global_belt_moving.get_value():
        time.sleep(0.2)

    return True


@uamethod
def start_demo(parent, movement, shelf):
    global storage
    global global_demonstrator_busy
    global global_panda_moving
    global global_belt_moving

    #if storage[shelf] is "0":  # can be activated - robot then only moves once per shelf number
    if False:
        return "Shelf empty - error!"

    elif not global_panda_moving.get_value() and not global_belt_moving.get_value():

        global_demonstrator_busy.set_value(True)
        move_thread = threading.Thread(name='move_demo_thread', target=start_demo_core, args=(movement, shelf,))
        move_thread.daemon = True
        move_thread.start()
        # storage[shelf] = "0"  # make shelf empty
        return "Successful"

    else:

        return "Shelf not empty - Demonstrator started!"


################################################# START #######################################################

if __name__ == "__main__":

    ################ CLIENT SETUP I ################

    client_panda = Client(global_url_panda_server)
    client_pixtend = Client(global_url_pixtend_server)
    client_fhs = Client(global_url_fhs_server)          # Original
    client_fhs_2 = Client(global_url_fhs_server)       # for the while loop inside the fh connection thread - otherwise there is an exception when getting variables from the server when simultaniously a
                                                        # datachange subscription routine is running
    # client_fhs = Client(global_url_pseudo_fhs_server)                                                                  # Testing with pseudo FH server
    # client = Client("opc.tcp://admin@localhost:4840/freeopcua/server/") #connect using a user

    ############# LOAD STORAGE DATA  #############
    # [1][2][3]
    # [4][5][6]
    # [7][8][9]
    with open("./dtz_storage", "r", encoding="utf-8") as input_file:
        for line in input_file:
            storage.append(line)

    # reconnection counter
    retry_counter = 1

    # counter for server loop in order to get notifications if it is running
    notification_counter = 0

    # keyboard interrupt raised by inner while loop giving signal also to outer while loop via variable
    keyboardint = False

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

    # start server
    server.start()
    logger.debug("OPC-UA - Master - Server started at {}".format(url))

    while not keyboardint:
        ###############   START SERVER   ###############

        try:
            # check if server is really running by setting one of the variables
            global_demonstrator_busy.set_value(True)
        except:

            try:
                logger.debug("Server not running?")
                server.start()
                logger.debug("OPC-UA - Master - Server started at {}".format(url))
            except:
                logger.debug("killing and restarting server")
                # server has a problem, so kill it and try again
                server.stop()
                server.start()


        time.sleep(0.3)
        #### CONNECT TO FH SERVER INSIDE A SEPARATE THREAD ####
        fh_connection_thread = threading.Thread(name='threaded_fh_connection', target=threaded_fh_connection, args=(), )
        fh_connection_thread.daemon = True
        fh_connection_thread.start()
        time.sleep(0.3)

        #### CONNECT TO PANDA SERVER INSIDE A SEPARATE THREAD ####
        panda_connection_thread = threading.Thread(name='threaded_fh_connection', target=threaded_panda_connection, args=(), )
        panda_connection_thread.daemon = True
        panda_connection_thread.start()
        time.sleep(0.3)

        #### CONNECT TO PIXTEND SERVER INSIDE A SEPARATE THREAD ####
        pixtend_connection_thread = threading.Thread(name='threaded_fh_connection', target=threaded_pixtend_connection, args=(), )
        pixtend_connection_thread.daemon = True
        pixtend_connection_thread.start()


        ###############  CHRIS' DATASTACK  ###############
        try:

            # Sending changed states to kafka stack
            tm = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
            # r1 = requests.post(url_opcua_adapter,data={'id': 'pandapc.panda_state', 'timestamp': tm, 'panda_state': panda_state})
            # r2 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_state', 'timestamp': tm, 'conbelt_state': conbelt_state})
            # r3 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_dist', 'timestamp': tm, 'conbelt_dist': conbelt_dist})

            ########################### RUNNNING LOOP ##############################
            # logger.debug("Starting and running...")

        except requests.exceptions.ConnectionError:
            logger.debug("Catched Exception: requests - connection to data stack")



        ############### SERVER RUNNING ROUTINE #######################
        logger.debug("Going into server loop")
        while True:

            time.sleep(0.5)

            try:
                # Every x seconds a message that the server is still running - could be irritating in the log file otherwise
                if notification_counter > 10:
                    logger.debug("server: running")
                    notification_counter = 0
                notification_counter = notification_counter + 1

                # logger.debug("panda moving: " + str(global_panda_moving.get_value()) + ". belt_moving: " + str(global_belt_moving.get_value()))
                # logger.debug("global_panda_moving: " + str(global_panda_moving.get_value()) + ". global_belt_moving: " + str(global_belt_moving.get_value()))

                if global_panda_moving.get_value() is True or global_belt_moving.get_value() is True:
                    global_object_pixtend.call_method("2:SwitchBusyLight", True)  # switch the alarm light to red - means the demonstrator is working
                    global_demonstrator_busy.set_value(True)
                else:
                    global_object_pixtend.call_method("2:SwitchBusyLight", False)  # switch the alarm light to green - means the demonstrator is not working
                    global_demonstrator_busy.set_value(False)

                # logger.debug("global_demonstrator busy: " + str(global_demonstrator_busy.get_value()))


            except Exception as e:
                logger.debug("server: Catched Exception: " + str(e))
                global_demonstrator_busy.set_value(True)

                logger.debug("server: it seems, that one of the clients is disconnected - waiting 5 seconds")
                time.sleep(5)



            except KeyboardInterrupt:
                logger.debug("\nCTRL+C pressed")
                server.stop()
                logger.debug("\nClients disconnected and Server stopped")
                keyboardint = True
                break

            # logger.debug(traceback.format_exc())
