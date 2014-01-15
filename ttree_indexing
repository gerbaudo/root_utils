#!/usr/bin/env python

''' quick proof-of-concept for indexed TChains '''

import sys
import argparse
import ROOT as r
import numpy as np

class IndexedChain(r.TChain):
  ''' See TChain::TChain() '''
  def __init__(self, *args):
    # just hand off all the arguments to TChain::TChain()
    r.TChain.__init__(self, *args)
    
    self.__index = np.array([], dtype=int)
  
  ''' Same as TChain::Add(), except this accepts an additional keyword argument:
  Add( ..., index=indexlist )
  
  where indexlist is a list of 0-indexed event numbers (relative to the input file).
  The index is adjusted to handle offsets if there are multiple TTree's in
  this chain.
  '''
  def Add(self, *args, **kwargs):
    initial_entries = self.GetEntries()
    r.TChain.Add(self, *args)
    
    # figure out how many entries did we just add
    new_entries = self.GetEntries() - initial_entries
    
    index = kwargs.get('index', [])
    # make sure the index is sorted, and cast to np array
    index_sorted = np.array(sorted(index), dtype=int)
    
    if max(index_sorted) >= new_entries:
      print >> sys.stderr, "Warning! Index extends beyond input file length. Truncating..."
      index_sorted = index_sorted[index_sorted<new_entries]
    
    # adjust the index offset
    index_sorted += initial_entries
    self.__index = np.hstack([self.__index, index_sorted])
  
  ''' Overload pyROOT's TChain iterator, skipping directly to the events indicated
  in the index
  '''
  def __iter__(self):
    for i in self.__index:
    n=self.GetEntry(i)
    if n == 0:
      # nothing read, we're done. but probably shouldn't have gotten
      # this far, if the indices are properly aligned.
      print >> sys.stderr, "Warning! Index runs beyond data size. Breaking iteration."
      break

if __name__ == "__main__":
  parser = argparse.ArgumentParser('barebones indexed tree demo')
  
  parser.add_argument('--treename', default='physics', help='the tree name to load from files')
  parser.add_argument('infile', nargs="+", help='the input root file(s).')
  
  args = parser.parse_args()
  
  # for illustration, just use an the same "index" for each file.
  # in practice you could load the index from a numpy file or something.
  index = [0,1,8,15,29,73,129,1234]
  
  # load up the files
  t = IndexedChain(args.treename)
  for f in args.infile:
   t.Add(f, index=index)
  print 'input tree has: %d entries'%t.GetEntries()
  
  for evt in t:
    # do something...
    pass