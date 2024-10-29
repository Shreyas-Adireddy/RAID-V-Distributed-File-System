# CHANGES:
# Added virtualtophysical() function for address calculations
# Made Put() compute parity data and work with multiple servers
# Made Get() work with multiple servers
# RSM stores data in server number NS-1 in the last block, prints which server lock is in
# SERVER DISCONNECTED messages for put and get
# At most once semantics for put and get
# Added logic to handle increasing numblocks 
# Use connection refuse error to handle disconnects
#Started working on adding error masking in Get and put

import pickle, logging
import fsconfig
import xmlrpc.client, socket, time

# --- NOV. 26 ---
# Added some logic for multiple servers by doing array of block server objects
# Changed Put() and Get() to SinglePut() and SingleGet()

debug = 0

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self):

        # initialize clientID
        if fsconfig.CID >= 0 and fsconfig.CID < fsconfig.MAX_CLIENTS:
            self.clientID = fsconfig.CID
        else:
            print('Must specify valid cid')
            quit()

        # initialize XMLRPC client connection to raw block server
        # if fsconfig.PORT:
        #     PORT = fsconfig.PORT
        # else:
        #     print('Must specify port number')
        #     quit()

        # -------------------------------
        # Code to handle multiple servers

        if fsconfig.NS <= 0:
            print("Mult specify valid number of servers")
            quit()
        else:
            self.NS = fsconfig.NS
        print("RSM SERVER: " + str(fsconfig.NS-1)) # Show server of RSM
        
        if fsconfig.STARTPORT <= 0:
            print("Must speciy valid startport ")
            quit()
        else: 
            self.startport = fsconfig.STARTPORT

        self.block_server = {}    
        for i in range(self.NS):
            PORT = self.startport + i
            server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(PORT)
            self.block_server[i] = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)
        # checking server blocks
        self.server_blocks_used =[[0] * 10 for _ in range(self.NS)]

        # End added code
        #--------------------------------
         

        socket.setdefaulttimeout(fsconfig.SOCKET_TIMEOUT)
        # initialize block cache empty
        self.blockcache =[[0] * fsconfig.TOTAL_NUM_BLOCKS for _ in range(self.NS)]
        # self.blockcache = {}

    ## Put: interface to write a raw block of data to the block indexed by block number
    ## Blocks are padded with zeroes up to BLOCK_SIZE

    def SinglePut(self, block_number, block_data, server_number):

        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # ljust does the padding with zeros
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            # Write block
            # commenting this out as the request now goes to the server
            # self.block[block_number] = putdata
            # call Put() method on the server; code currently quits on any server failure
            # rpcretry = True
            # while rpcretry:
            #     rpcretry = False
            #     try:
            #         if debug == 1:
            #             print("SERVER NUMBER: " + str(server_number)) # DEBUG
            #         ret = self.block_server[server_number].Put(block_number, putdata)
            #     except socket.timeout:
            #         print("SERVER_TIMED_OUT")
            #         time.sleep(fsconfig.RETRY_INTERVAL)
            #         rpcretry = True


            #At most-once semantics
            try:
                if debug == 1:
                    print("SERVER NUMBER: " + str(server_number) + " putdata: " + str(putdata.decode())) # DEBUG
                # print("PUT " + str(block_number) + " " + str(putdata.decode()) + " server: " + str(server_number))
                ret = self.block_server[server_number].Put(block_number, putdata)
            except ConnectionRefusedError:
                #print("SERVER DISCONNECTED PUT " + str(block_number))
                #time.sleep(fsconfig.RETRY_INTERVAL)
                return -1
                    
            # update block cache
            if fsconfig.LOGCACHE == 1:
                print('CACHE_WRITE_THROUGH ' + str(block_number))
            self.blockcache[server_number][block_number] = putdata

            # if debug == 1:
            #     print("printing cache:")
            #     for server_number, blocks in self.blockcache.items():
            #         for block_number, block_data in blocks.items():
            #             print("Server Number:", server_number, "Block Number:", block_number, "Block Data:", block_data.decode())
            
            # flag this is the last writer
            # unless this is a release - which doesn't flag last writer
            if block_number != fsconfig.TOTAL_NUM_BLOCKS-1:

                # getting last writer block
                LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS//fsconfig.NS - 2

                # need to translate to physical block since we can't recursively call get()
                phys_address, server_number_last_writer, _ = self.VirtualToPhysical(LAST_WRITER_BLOCK)
                updated_block = bytearray(fsconfig.BLOCK_SIZE)
                updated_block[0] = self.clientID
                
                # rpcretry = True
                # while rpcretry:
                #     rpcretry = False
                #     try:
                #         self.block_server[server_number].Put(LAST_WRITER_BLOCK, updated_block)
                #     except socket.timeout:
                #         #print("SERVER_TIMED_OUT")
                #         print("SERVER DISCONNECTED PUT " + str(block_number))
                #         time.sleep(fsconfig.RETRY_INTERVAL)
                #         rpcretry = True

                #At most-once semantics
                try:
                    # writing to the location where last writer is stored
                    self.block_server[server_number_last_writer].Put(phys_address, updated_block)
                except ConnectionRefusedError:
                    #print("SERVER_TIMED_OUT")
                    #print("SERVER DISCONNECTED PUT " + str(LAST_WRITER_BLOCK))
                    #time.sleep(fsconfig.RETRY_INTERVAL)
                    return -1

            if ret == -1:
                logging.error('Put: Server returns error')
                quit()
            return 0
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def SingleGet(self, block_number, server_number):

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # logging.debug ('\n' + str((self.block[block_number]).hex()))
            # commenting this out as the request now goes to the server
            # return self.block[block_number]
            # call Get() method on the server
            # don't look up cache for last two blocks
            # check if block_number is within a valid range and is in the blockcache of the server
            if (block_number < fsconfig.TOTAL_NUM_BLOCKS-2) and (self.blockcache[server_number][block_number] != 0):
                if fsconfig.LOGCACHE == 1:
                    print('CACHE_HIT '+ str(block_number))
                data = self.blockcache[server_number][block_number]
                if debug == 1:
                    print("got from cache-server: " + str(server_number) + " block: " + str(block_number) + " data: " + str(data.decode()))
            else:
                if fsconfig.LOGCACHE == 1:
                    print('CACHE_MISS ' + str(block_number))
                # rpcretry = True
                # while rpcretry:
                #     rpcretry = False
                #     try:
                #         data = self.block_server[server_number].Get(block_number)
                #     except socket.timeout:
                #         print("SERVER_TIMED_OUT")
                #         time.sleep(fsconfig.RETRY_INTERVAL)
                #         rpcretry = True

            #At most-once semantics
                try:
                    data = self.block_server[server_number].Get(block_number)
                    if debug == 1:
                        print("got from server: " + str(server_number) + " block: " + str(block_number) + " data: " + str(data.decode()))
                except ConnectionRefusedError:
                    #print("SERVER_TIMED_OUT")
                    #time.sleep(fsconfig.RETRY_INTERVAL)
                    return -1

                if data == -2: # corrupted block
                    return -2
                    
            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()


    def VirtualToPhysical(self, block_number):
        block_group = block_number//(fsconfig.NS-1) # Example: Block[0] in all servers are group 0, Block[1] in all servers is group 1, ect.
        parity_block_position = block_group % fsconfig.NS # Position in group for parity block. Making it diagonal. Shown in L26 Zoom
        put_block_position = block_number % (fsconfig.NS - 1) # Position of data we are writing
        
        if(put_block_position < parity_block_position):
            put_block_position = put_block_position
        else:
            put_block_position = put_block_position + 1 
                # Shift position when put_block_position is greater than or equal to parity_block_position

        phys_address = block_group
        server_number = put_block_position
        parity_server = parity_block_position

        return phys_address, server_number, parity_server


    # ----- TESTING RAID 4/5 Put/Get----- 
    def Put(self, block_number, block_data):

        # converting vitual block number to physical address
        phys_address, server_number, parity_server = self.VirtualToPhysical(block_number)

        # getting previous data block and previous parity block
        old_block_data = self.SingleGet(phys_address, server_number)

        # if corrupt block or if server disconnected, get the old block anyways
        if old_block_data == -1:
            
            print("SERVER_DISCONNECT " + str(server_number))
            
            # get corrected data
            corrected_data = bytearray(fsconfig.BLOCK_SIZE)
            for i in range(fsconfig.NS): # XORs all data together
                if i != server_number:
                    other_data = self.SingleGet(phys_address, i)
                    corrected_data = bytearray(a ^ b for (a,b) in zip(corrected_data, other_data))
            
            #Assign data to be corrected data
            old_block_data = corrected_data
        
        elif old_block_data == -2:
            
            print("CORRUPTED_BLOCK " + str(block_number))
            
            # get corrected data
            corrected_data = bytearray(fsconfig.BLOCK_SIZE)
            for i in range(fsconfig.NS): # XORs all data together
                if i != server_number:
                    other_data = self.SingleGet(phys_address, i)
                    corrected_data = bytearray(a ^ b for (a,b) in zip(corrected_data, other_data))
            
            #Assign data to be corrected data
            old_block_data = corrected_data
        
        old_parity_data = self.SingleGet(phys_address, parity_server)

        # same if the parity server is corrupt/disconnected
        # if corrupt block or if server disconnected, get the old block anyways
        if old_parity_data == -1:
            
            print("SERVER_DISCONNECT " + str(server_number))
            
            # get corrected data
            corrected_data = bytearray(fsconfig.BLOCK_SIZE)
            for i in range(fsconfig.NS): # XORs all data together
                if i != server_number:
                    other_data = self.SingleGet(phys_address, i)
                    corrected_data = bytearray(a ^ b for (a,b) in zip(corrected_data, other_data))
            
            #Assign data to be corrected data
            old_parity_data = corrected_data
        
        if old_parity_data == -2:
            
            print("CORRUPTED_BLOCK " + str(block_number))
            
            # get corrected data
            corrected_data = bytearray(fsconfig.BLOCK_SIZE)
            for i in range(fsconfig.NS): # XORs all data together
                if i != server_number:
                    other_data = self.SingleGet(phys_address, i)
                    corrected_data = bytearray(a ^ b for (a,b) in zip(corrected_data, other_data))
            
            #Assign data to be corrected data
            old_parity_data = corrected_data

        new_parity_data = bytearray(a ^ b ^ c for (a,b,c) in zip(block_data, old_block_data, old_parity_data)) 
        # XOR New data with old data and old parity to obtain new parity
        
        parity_success = self.SinglePut(phys_address, new_parity_data, parity_server) # Put parity data
        data_success = self.SinglePut(phys_address, block_data, server_number) # Put block data

        if data_success == -1:
            print("SERVER DISCONNECTED PUT" + str(block_number))
        if parity_success == -1:
            print("SERVER DISCONNECTED PARITY PUT")
        return 0
    
    def Get(self, block_number):
        # data = []
        # for i in range(self.NS):
        #     #print(i)
        #     data.append(self.SingleGet(block_number, i))
        
        #Check if equal
        # if([data[0]]*len(data) == data):
        #     #print("All servers equal")
        #     return data[0]
        # else:
        #     print("Servers not equal")
        #     return -1
        
        # server_number = block_number % self.NS # Equally distributes puts and gets between all servers
        # server_block_number = block_number % fsconfig.TOTAL_NUM_BLOCKS # #Determine block to put in each server
        # data = self.SingleGet(server_block_number, server_number)
        # print("in get()")
        phys_address, server_number, parity_server = self.VirtualToPhysical(block_number)
        # print("Getting block " + str(block_number) + " in server " + str(server_number) + " and parity in server " + str(parity_server) + " and phys address " + str(phys_address))
        data = self.SingleGet(phys_address, server_number)
        
        # server disconnect handling
        if data == -1:
            print("SERVER DISCONNECTED GET " + str(block_number))

            corrected_data = bytearray(fsconfig.BLOCK_SIZE) # Byte array of all zeros
            for i in range(fsconfig.NS): # XORs all data together
                if i != server_number:
                    other_data = self.SingleGet(phys_address, i)
                    corrected_data = bytearray(a ^ b for (a,b) in zip(corrected_data, other_data))
            
            #Assign data to be corrected data
            data = corrected_data

        # corrupt data handling
        elif data == -2:
            print("CORRUPTED_BLOCK " + str(block_number))
            
            corrected_data = bytearray(fsconfig.BLOCK_SIZE) # Byte array of all zeros
            for i in range(fsconfig.NS): # XORs all data together
                if i != server_number:
                    other_data = self.SingleGet(phys_address, i)
                    corrected_data = bytearray(a ^ b for (a,b) in zip(corrected_data, other_data))

            #Assign data to be corrected data
            data = corrected_data

        return data
    
    # ----- END RAID 4/5 -----

## RSM: read and set memory equivalent

    def RSM(self, block_number):
        logging.debug('RSM: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            rpcretry = True
            while rpcretry:
                rpcretry = False
                try:
                    data = self.block_server[fsconfig.NS-1].RSM(block_number)
                except ConnectionRefusedError:
                    print("SERVER_TIMED_OUT")
                    time.sleep(fsconfig.RETRY_INTERVAL)
                    rpcretry = True

            return bytearray(data)

        logging.error('RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

    # Mult-Server RSM
    # def RSM(self, block_number):
    #     data = []
    #     for i in range(fsconfig.NS):
    #         data.append(self.SingleRSM(block_number,i))

    #     if([data[0]]*len(data) == data):
    #         #print("All servers equal")
    #         return data[0]
    #     else:
    #         print("Servers not equal")
    #         return -1

        ## Acquire and Release using a disk block lock

    def Acquire(self):
        logging.debug('Acquire')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS//fsconfig.NS - 1
        lockvalue = self.RSM(RSM_BLOCK);
        logging.debug("RSM_BLOCK Lock value: " + str(lockvalue))
        while lockvalue[0] == 1:  # test just first byte of block to check if RSM_LOCKED
            logging.debug("Acquire: spinning...")
            lockvalue = self.RSM(RSM_BLOCK);
        # once the lock is acquired, check if need to invalidate cache
        self.CheckAndInvalidateCache()
        return 0

    def Release(self):
        logging.debug('Release')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS//fsconfig.NS - 1
        # Put()s a zero-filled block to release lock
        self.SinglePut(RSM_BLOCK,bytearray(fsconfig.RSM_UNLOCKED.ljust(fsconfig.BLOCK_SIZE, b'\x00')),fsconfig.NS-1)
        return 0

    def CheckAndInvalidateCache(self):
        
        # getting last writer block
        LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS//fsconfig.NS - 2
        last_writer = self.Get(LAST_WRITER_BLOCK)
        
        if debug == 1:
            print("last writer:" + str(last_writer) + " my CID:" + str(self.clientID))
        
        # if ID of last writer is not self, invalidate and update
        if last_writer[0] != self.clientID:
            if fsconfig.LOGCACHE == 1:
                print("CACHE_INVALIDATED")
        
            # clear cache
            self.blockcache =[[0] * fsconfig.TOTAL_NUM_BLOCKS for _ in range(self.NS)]
        

    ## Serializes and saves the DiskBlocks block[] data structure to a "dump" file on your disk

    def DumpToDisk(self, filename):

        logging.info("DiskBlocks::DumpToDisk: Dumping pickled blocks to file " + filename)
        file = open(filename,'wb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)
        pickle.dump(file_system_constants, file)
        pickle.dump(self.block, file)

        file.close()

    ## Loads DiskBlocks block[] data structure from a "dump" file on your disk

    def LoadFromDump(self, filename):

        logging.info("DiskBlocks::LoadFromDump: Reading blocks from pickled file " + filename)
        file = open(filename,'rb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)

        try:
            read_file_system_constants = pickle.load(file)
            if file_system_constants != read_file_system_constants:
                print('DiskBlocks::LoadFromDump Error: File System constants of File :' + read_file_system_constants + ' do not match with current file system constants :' + file_system_constants)
                return -1
            block = pickle.load(file)
            for i in range(0, fsconfig.TOTAL_NUM_BLOCKS):
                self.Put(i,block[i])
            return 0
        except TypeError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered type error ")
            return -1
        except EOFError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered EOFError error ")
            return -1
        finally:
            file.close()


## Prints to screen block contents, from min to max

    def PrintBlocks(self,tag,min,max):
        print ('#### Raw disk blocks: ' + tag)
        for i in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))
