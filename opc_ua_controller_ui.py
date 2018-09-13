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
from opcua import Client
import pytz
import requests
import time
import sys

sys.path.insert(0, "..")




desired_distance = 0.55  # distance in meters to drive the belt
belt_velocity = 0.05428  # velocity of the belt in m/s (5.5cm/s)
timebuffer = 3           # time buffer for the wait loops after method call. wifi istn that fast
storage = []             # our storage data as an array

def move_belt(direction, distance):
    object1_pixtend.call_method("2:MoveBelt", direction, desired_distance)  # drive 55cm right
    print("called move_belt to " + str(direction) + " for " + str(desired_distance) + "m")
    print("sleeping...")
    for i in range(0, (int(desired_distance / belt_velocity) * 10) + timebuffer):
        time.sleep(0.1)


def move_robot(movement, place):
    object1_panda.call_method("MoveRobot", movement, place)  # movement = PO,SO or PS # place = 1-9
    if movement == "PO":
        print("called move_robot from Printer to Output")
    elif movement == "SO":
        print("called move_robot from Storage #" + place + " to Output")
    elif movement == "PS":
        print("called move_robot from Printer to Storage #" + place)
    for i in range(0, (int(desired_distance / belt_velocity) * 10) + timebuffer):
        time.sleep(0.1)


if __name__ == "__main__":

    url_opcua_adapter = "192.168.48.81:1337"
    client_panda = Client("opc.tcp://192.168.48.41:4840/freeopcua/server/")
    client_pixtend = Client("opc.tcp://192.168.48.42:4840/freeopcua/server/")
    # client = Client("opc.tcp://admin@localhost:4840/freeopcua/server/") #connect using a user

    try:
        client_panda.connect()
        client_pixtend.connect()

        # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects
        root_panda = client_panda.get_root_node()
        root_pixtend = client_pixtend.get_root_node()

        # get a specific node knowing its node id
        # var = client.get_node(ua.NodeId(1002, 2))
        # var = client.get_node("ns=3;i=2002")
        # print(var)
        # var.get_data_value() # get value of node as a DataValue object
        # var.get_value() # get value of node as a python builtin
        # var.set_value(ua.Variant([23], ua.VariantType.Int64)) #set node value using explicit data type
        # var.set_value(3.9) # set node value using implicit data type


        # Now getting a variable node using its browse path
        # server_time = root.get_child(["0:Objects", "2:Object1", "2:ServerTime"])
        object1_panda = root_panda.get_child(["0:Objects", "2:Object1"])
        object1_pixtend = root_pixtend.get_child(["0:Objects", "2:Object1"])

        mover_panda = root_panda.get_child(["0:Objects", "2:Object1", "2:MoveRobot"])
        mover_pixtend = root_pixtend.get_child(["0:Objects", "2:Object1", "2:MoveBelt"])


        while True:
            # VALUES
            panda_state = root_panda.get_child(["0:Objects", "2:Object1", "2:RobotState"])
            # panda_temp_value = root_panda.get_child(["0:Objects", "2:Object1", "2:RobotTempValue"])
            conbelt_state = root_pixtend.get_child(["0:Objects", "2:Object1", "2:ConBeltState"])
            conbelt_dist = root_pixtend.get_child(["0:Objects", "2:Object1", "2:ConBeltDist"])

            tm = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
            r1 = requests.post(url_opcua_adapter, data={'id': 'pandapc.panda_state', 'timestamp': tm, 'panda_state': panda_state})
            r2 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_state', 'timestamp': tm, 'conbelt_state': conbelt_state})
            r3 = requests.post(url_opcua_adapter, data={'id': 'pandapc.conbelt_dist', 'timestamp': tm, 'conbelt_dist': conbelt_dist})

            time.sleep(0.3)




        # STORAGE
        # get storage data from file
        with open("dtz_storage", "r", encoding="utf-8") as inputfile:
            for line in inputfile:
                storage.append(line)






        # METHOD CALLS
        move_robot("SO", 1) # move robot from Storage to Output
        move_belt("left", 0.55)


    except KeyboardInterrupt:
        print("\nClient stopped")
    except requests.exceptions.ConnectionError:
    print("error connecting...")
    finally:
        client_pixtend.disconnect()
        client_panda.disconnect()

        # write storage data to file
        with open("dtz_storage", "w", encoding="utf-8") as outputfile:
            i = 0
            for line in outputfile:
                outputfile.write(storage[i])
                i = i + 1



