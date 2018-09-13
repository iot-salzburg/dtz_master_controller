#      _____         __        __                               ____                                        __
#     / ___/ ____ _ / /____   / /_   __  __ _____ ____ _       / __ \ ___   _____ ___   ____ _ _____ _____ / /_
#     \__ \ / __ `// //_  /  / __ \ / / / // ___// __ `/      / /_/ // _ \ / ___// _ \ / __ `// ___// ___// __ \
#    ___/ // /_/ // /  / /_ / /_/ // /_/ // /   / /_/ /      / _, _//  __/(__  )/  __// /_/ // /   / /__ / / / /
#   /____/ \__,_//_/  /___//_.___/ \__,_//_/    \__, /      /_/ |_| \___//____/ \___/ \__,_//_/    \___//_/ /_/
#                                              /____/
#   Salzburg Research ForschungsgesmbH
#   Armin Niedermueller

#   OPC UA Server on Master Cluster
#   The purpose of this OPCUA client is to  readtheir (state) variables and ...


from datetime import datetime
from opcua import Client
import pytz
import time
import sys

sys.path.insert(0, "..")


desired_distance = 0.55  # distance in meters to drive the belt
belt_velocity = 0.05428  # velocity of the belt in m/s (5.5cm/s)
timebuffer = 3           # time buffer for the wait loops after method call. wifi istn that fast
storage = []             # our storage data as an array

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
        object1_panda = root_panda.get_child(["0:Objects", "2:Panda"])
        object1_pixtend = root_pixtend.get_child(["0:Objects", "2:Object1"])

        mover_panda = root_panda.get_child(["0:Objects", "2:Object1", "2:MoveRobot"])
        mover_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt, "2:MoveBelt"])


        while True:
            # VALUES
            panda_state = root_panda.get_child(["0:Objects", "2:Panda", "2:RobotState"])
            # panda_temp_value = root_panda.get_child(["0:Objects", "2:Panda", "2:RobotTempValue"])
            conbelt_state = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltState"])
            conbelt_dist = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltDist"])

                                                
                                                
                                                
            time.sleep(0.3)

    except KeyboardInterrupt:
        print("\nClient stopped")
    except requests.exceptions.ConnectionError:
    print("error connecting...")
    finally:
        client_pixtend.disconnect()
        client_panda.disconnect()



