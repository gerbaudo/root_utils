#!/usr/bin/env python

import hashlib
import logging
import os
import sys
import unittest

import ROOT as r

class IndexedChain(r.TChain):
  """
  A chain that keeps track of the entries passing some selections.

  The idea is that you run once on all events, and a TEntryList
  is cached for each selection. On the following runs one can
  loop on the interesting entries only.

  Each selection is identified by a TCut, but the TCut itself is only
  used as a bookkeping tool (for example it can contain variables that
  are computed on the fly, not present in the tree).

  Example usage:

    chain = IndexedChain(treename)
    chain.Add(filename)
    chain.retrieve_entrylists([TCut('cut1','x>0'),
                               TCut('cut2','y>3')])

    # we know our entries: loop on cut, then loop on entry
    for cut in chain.tcuts_with_existing_list():
      chain.preselect(cut)
      for entry in chain:
        # fill histograms

    # we don't know our entries: loop on entry then loop on cut
    chain.preselect(None)
    for ientry, entry in enumerate(chain):
      for cut in chain.tcuts_without_existing_list():
        # here you must assign all the variables needed by tcut
        cut_name, cut_expr = cut.GetName(), cut.GetTitle()
        pass_cut = eval(cut_expr)
        if pass_cut:
         # fill histograms
         chain.add_entry_to_list(cut, ientry)

    chain.save_lists()

  davide.gerbaudo@gmail.com
  Jan 2015
  """
  def __init__(self, *args):
    """
    See TChain::TChain()
    """
    r.TChain.__init__(self, *args)
    self.__current_entrylist = None
    self.__has_entry_list = dict()
    self.__entry_list = dict()
    self.__entry_list_file = dict()
    self.cache_directory = './cache_IndexedChain'
    self.hash_func = hashlib.md5
    # logging.basicConfig(filename='indexed_chain.log',level=logging.INFO)
    logging.basicConfig(filename='indexed_chain.log',level=logging.DEBUG)
    self.logger = logging.getLogger(__name__)

  def __iter__(self):
    """
    Overload pyROOT's TChain iterator, skipping directly to the events
    indicated in the TEntrylist
    """
    if self.__current_entrylist:
      ientry = self.__current_entrylist.Next()
      while ientry>=0:
        self.GetEntry(ientry)
        ientry = self.__current_entrylist.Next()
        yield self
    else:
      for ientry in xrange(self.GetEntries()):
        self.GetEntry(ientry)
        yield self

  def retrieve_entrylists(self, tcuts=[]):
    """
    Get the existing entry lists for a given list of TCuts.

    Loop over the TCuts, and determine whether the corresponding
    TEntryList is already available. When they are, store them in
    the entry_list dict. Both TFiles and TEntryList are indexed by
    'tcut_filename', encoding both selection and files.
    """
    self.check_cache_dir()
    self.__tcuts = tcuts
    for tcut in tcuts:
      fname = self.tcut_filename(tcut)
      if os.path.exists(fname):
        entrylist_file = r.TFile.Open(fname)
        self.__has_entry_list[fname] = True
        self.__entry_list_file[fname] = entrylist_file
        self.__entry_list[fname] = entrylist_file.Get(tcut.GetName())
        self.logger.info("retrieved entry list for '%s' from '%s'" % (tcut.GetName(), fname))
      else:
        self.__has_entry_list[fname] = False
        self.__entry_list[fname] = r.TEntryList(tcut.GetName(), self.string_to_be_hashed(tcut))
        self.logger.info("creating entry list for '%s'" % tcut.GetName())

  def tcuts_with_existing_list(self):
    return [t for t in self.__tcuts if self.__has_entry_list[self.tcut_filename(t)]]

  def tcuts_without_existing_list(self):
    return [t for t in self.__tcuts if not self.__has_entry_list[self.tcut_filename(t)]]

  def preselect(self, tcut):
    key = self.tcut_filename(tcut) if tcut else None
    if key in self.__entry_list and self.__has_entry_list[key]:
      entry_list = self.__entry_list[key]
      self.__current_entrylist = entry_list
      self.logger.info("preselected %d events (out of %d) for cut '%s'" % (entry_list.GetN(),
                                                                           self.GetEntries(),
                                                                           tcut.GetName()))
    else:
      self.__current_entrylist = None
      if tcut:
        self.logger.warning("requested entry list for cut '%s' not available" % tcut.GetName())
      else:
        self.logger.info("no preselection: %d events" % self.GetEntries())

  def num_events_preselected(self):
    return 0 if not self.__current_entrylist else self.__current_entrylist.GetN()

  def add_entry_to_list(self, tcut, ientry):
    key = self.tcut_filename(tcut)
    self.__entry_list[key].Enter(ientry)

  def save_lists(self):
    self.check_cache_dir()
    for tcut in self.tcuts_without_existing_list():
      filename = self.tcut_filename(tcut)
      key = filename
      entrylist = self.__entry_list[key]
      outfile = r.TFile(filename, 'recreate')
      outfile.cd()
      entrylist.SetDirectory(outfile)
      entrylist.Write(entrylist.GetName())
      outfile.Close()
      self.logger.info("wrote entry list for '%s' to %s" % (tcut.GetName(), filename))
#
# internal functions
#____________________________________________________________
  @property
  def filenames(self):
    """
    cache filenames so that we don't need to always call GetListOfFiles
    """
    if hasattr(self, '_filenames'):
      return self._filenames
    else:
      return [f.GetName() for f in self.GetListOfFiles()]

  def mkdir_if_needed(self, dirname):
    dest_dir = None
    if os.path.exists(dirname) and os.path.isdir(dirname) :
      dest_dir = dirname
    elif not os.path.exists(dirname) :
      os.makedirs(dirname)
      dest_dir = dirname
    return dest_dir

  def check_cache_dir(self):
    """
    Check whether the cache directory exists; if not, make one
    """
    self.mkdir_if_needed(self.cache_directory)
    assert os.path.isdir(self.cache_directory),"invalid cache directory '{}'".format(self.cache_directory)

  def delete_entrylists(self, tcuts=[]):
    """
    Delete existing entry lists for a given list of cuts
    Note-to-self: currently not clearing dicts, so need to create a new object
    """
    if not tcuts:
        self.logger.info("calling delete_entrylists without cuts...will do nothing")
    for tcut in tcuts:
      fname = self.tcut_filename(tcut)
      if os.path.exists(fname):
        os.remove(fname)
        self.logger.info("deleted existing entrylist for cut '%s' %s" % (tcut.GetName(), fname))

  def string_to_be_hashed(self, tcut):
    """
    Encode a given selection (cutname + cutexpression + filenames) in a string"
    """
    return ''.join([tcut.GetName(), tcut.GetTitle(), self.GetName()] +self.filenames)

  def hash(self, tcut):
    return self.hash_func(self.string_to_be_hashed(tcut)).hexdigest()

  def tcut_filename(self, tcut):
    """
    Given a TCut, provide the filename where the corresponding entry list is stored
    """
    return os.path.join(self.cache_directory, tcut.GetName()+'_'+self.hash(tcut)+'.root')

#
# testing
#____________________________________________________________

def dummy_filename() : return "/tmp/dummy_file.root"
def dummy_treename() : return "dummy_tree"
def create_dummy_tree():
  import array
  out_file = r.TFile.Open(dummy_filename(), 'recreate')
  out_file.cd()
  tree = r.TTree(dummy_treename(), 'an example dummy tree')
  lv = r.TLorentzVector()
  x = array.array( 'd', 1*[ 0. ] )
  y = array.array( 'd', 1*[ 0. ] )
  tree.Branch('x', x, 'x[1]/D')
  tree.Branch('y', y, 'y[1]/D')
  for i in range(1000):
    x[0] = i
    y[0] = 2*i
    tree.Fill()
  tree.Write()
  out_file.Close()


def even_cut():
  return r.TCut("even entries", "x%2==0")

def odd_cut():
  return r.TCut("odd entries", "x%2!=0")

class TestEntryList(unittest.TestCase) :
  def test_witout_entrylist(self):
    infile = r.TFile.Open(dummy_filename())
    tree = infile.Get(dummy_treename())
    n_entries_available = tree.GetEntries()
    n_entries_processed = 0
    for entry in tree:
      n_entries_processed +=1
    self.assertEqual(n_entries_processed, n_entries_available)
    infile.Close()

  def test_first_run_with_absent_entrylist(self):
    cut = even_cut()
    chain = IndexedChain(dummy_treename())
    chain.Add(dummy_filename())
    chain.delete_entrylists([cut]) # make sure we start from scratch
    chain.retrieve_entrylists([cut])
    n_entries_available = chain.GetEntries()
    n_entries_processed = 0
    for entry in chain:
      n_entries_processed +=1
    self.assertEqual(n_entries_processed, n_entries_available)

  def test_with_entrylist(self):
    cut = even_cut()
    chain = IndexedChain(dummy_treename())
    chain.Add(dummy_filename())
    chain.delete_entrylists([cut])
    chain.retrieve_entrylists([cut])
    cutstring = cut.GetTitle()

    # run once to fill the entry list
    n_entries_available = chain.GetEntries()
    n_entries_processed = 0
    n_entries_passing = 0
    for ientry, entry in enumerate(chain):
      x = entry.x
      pass_cut = eval(cutstring)
      if pass_cut:
        n_entries_passing +=1
        chain.add_entry_to_list(cut, ientry)
      n_entries_processed +=1
    chain.save_lists()
    self.assertEqual(n_entries_processed, n_entries_available)

    # run once to use the entry list
    n_entries_available = chain.GetEntries()
    n_entries_processed = 0
    n_entries_passing = 0
    chain.retrieve_entrylists([cut])
    chain.preselect(cut)
    n_entries_selected = chain.num_events_preselected()
    for entry in chain:
      x = entry.x
      pass_cut = eval(cutstring)
      if pass_cut:
        n_entries_passing +=1
      n_entries_processed +=1
    self.assertEqual(n_entries_processed, n_entries_selected)

  def test_with_partial_entrylist(self):
    chain = IndexedChain(dummy_treename())
    chain.Add(dummy_filename())

    # clear things up
    cuts = [even_cut(), odd_cut()]
    chain.delete_entrylists(cuts)
    # run once and just count
    counters_pre = dict((c.GetName(), 0) for c in cuts)
    counters_post = dict((c.GetName(), 0) for c in cuts)
    for ientry, entry in enumerate(chain):
      x = entry.x
      for cut in cuts:
        cut_name = cut.GetName()
        cut_expr = cut.GetTitle()
        if eval(cut_expr):
          counters_pre[cut_name] += 1

    # run once and fill the list for only one cut
    cuts = [even_cut()]
    chain.retrieve_entrylists(cuts)
    for ientry, entry in enumerate(chain):
      x = entry.x
      for cut in cuts:
        cut_name = cut.GetName()
        cut_expr = cut.GetTitle()
        if eval(cut_expr):
          chain.add_entry_to_list(cut, ientry)
    chain.save_lists()

    # run again, but now with two cuts, one of which has an entrylist
    cuts = [even_cut(), odd_cut()]
    chain = IndexedChain(dummy_treename()) # new obj, see note in delete_entrylists
    chain.Add(dummy_filename())
    chain.retrieve_entrylists(cuts)

    for cut in chain.tcuts_with_existing_list():
      chain.preselect(cut)
      for entry in chain:
        counters_post[cut.GetName()] += 1

    chain.preselect(None)
    for ientry, entry in enumerate(chain):
      x = entry.x
      for cut in chain.tcuts_without_existing_list():
        cut_name = cut.GetName()
        cut_expr = cut.GetTitle()
        if eval(cut_expr):
          counters_post[cut_name] += 1

    self.assertEqual(counters_pre, counters_post)


if __name__ == "__main__":
  create_dummy_tree()
  unittest.main()
