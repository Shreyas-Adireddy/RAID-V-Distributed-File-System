# CHANGES:
# Added virtualtophysical() function for address calculations
# Made Put() compute parity data and work with multiple servers
# Made Get() work with multiple servers

import pickle, logging
import argparse
import time
import fsconfig

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import hashlib

debug = 0

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)

class DiskBlocks():
  def __init__(self, total_num_blocks, block_size, delayat, cblk):
    # This class stores the raw block array
    self.block = []
    # initialize request counter
    self.counter = 0
    self.quicktest = 0
    self.delayat = delayat
    self.cblk = cblk
    # Initialize raw blocks
    for i in range (0, total_num_blocks):
      putdata = bytearray(block_size)
      self.block.insert(i,putdata)

    self.checksums = {}
    # initialize checksum object
    hasher = hashlib.md5()
    # calculate checksum of no data
    hasher.update(self.block[total_num_blocks-5])
    checksum = hasher.hexdigest()
    # iterate through each block and store checksum of no data
    for i in range (0, total_num_blocks):
      self.checksums[i] = checksum

  def Sleep(self):
    self.counter += 1
    if (self.counter % self.delayat) == 0:
      time.sleep(10)

if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-delayat', '--delayat', type=int, help='an integer value')
  ap.add_argument('-cblk', '--cblk', type=int, help='an integer value')

  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks')
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT = args.port
  else:
    print('Must specify port number')
    quit()

  if args.delayat:
    delayat = args.delayat
  else:
    # initialize delayat with artificially large number
    delayat = 1000000000

  if args.cblk:
    cblk = args.cblk
  else:
    # initialize cblk with artificially large number
    cblk = 1000000000
  

  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE, delayat, cblk)

  # Create server
  server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)


  def Get(block_number):

    # initializing checksum thing
    hasher = hashlib.md5()
    
    # fetching data
    result = RawBlocks.block[block_number]
    
    if debug == 1:
      print("get block_number: " + str(block_number))
    
    # calculate checksum
    hasher.update(result)
    checksum = hasher.hexdigest()

    if debug == 1:
      print("get checksum: " + str(checksum) + " data: " + str(result))
    
    # doing a try because sometimes clients read from a block
    # before it's been written to, so it doesn't have a checksum
    # try:
    saved_checksum = RawBlocks.checksums[block_number]
    # except:
    #   saved_checksum = 0

    # checking if checksum is correct, if not return corruption error
    if checksum != saved_checksum:
      if debug == 1:
        print("Checksum failed for block " + str(block_number))
      result = -2

    RawBlocks.Sleep()
    return result

  server.register_function(Get)


  def Put(block_number, data):
    
    # initializing checksum thing
    hasher = hashlib.md5()
    
    # storing data
    RawBlocks.block[block_number] = data.data

    if debug == 1:
      print("put block_number: " + str(block_number) + " data: " + str(data.data.decode()))
    
    # calculate checksum
    hasher.update(data.data)
    checksum = hasher.hexdigest()
    
    # checking if checksum is correct, had problems with this initially
    if debug == 1:
      print("put checksum: " + str(checksum) + " data: " + str(data.data))
      hasher = hashlib.md5()
      hasher.update(data.data)
      checksum = hasher.hexdigest()
      print("put checksum2: " + str(checksum) + " data: " + str(data.data))

    # storing checksum
    RawBlocks.checksums[block_number] = checksum
    
    # implementing faux decay for given cblk by replacing data
    if cblk == block_number:
      if debug == 1:
        print("replaced checksum!!!!!!! block: " + str(block_number))

      # corrupting data 
      RawBlocks.block[block_number] = b'\x12\x32\x21\x02\x83' + data.data[:5]

      if debug == 1:
        hasher = hashlib.md5()
        hasher.update(b'\x12\x32\x21\x02\x83' + data.data[:5])
        print("messed up hash: " + str(hasher.hexdigest()))
    
    RawBlocks.Sleep()
    return 0

  server.register_function(Put)

# instructions said to assume server doing RSM is never corrupted, so no checksum
  def RSM(block_number):
    
    RSM_LOCKED = bytearray(b'\x01') * 1
    result = RawBlocks.block[block_number]
    # RawBlocks.block[block_number] = RSM_LOCKED
    RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
    RawBlocks.Sleep()
    return result

  server.register_function(RSM)

  # Run the server's main loop
  print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
  server.serve_forever()