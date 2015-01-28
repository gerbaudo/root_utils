# indexed_chain
A TChain with selection indices

The idea is that you run once on all events, and a TEntryList
is cached for each selection. On the following runs one can
loop on the interesting entries only.

  
Each selection is identified by a TCut, but the TCut itself is only
used as a bookkeping tool (for example it can contain variables that
are computed on the fly, not present in the tree).

  Example usage:
```
chain = IndexedChain(treename)
chain.Add(filename)
chain.retrieve_entrylists([cut1, cut2, cut3])

# loop on cut, then loop on entry
for cut in chain.tcuts_with_existing_list():
  chain.preselect(cut)
  for event in chain:
    # fill histograms

# loop on entry then loop on cut
for ientry in xrange(chain.GetEntries()):
  chain.GetEntry(ientry)
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
