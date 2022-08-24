from dataclasses import dataclass
from pprint import pprint

import cyclonedds.idl as idl
import cyclonedds.idl.types as types
import cyclonedds.idl.annotations as annotate
from cyclonedds.core import Listener
from cyclonedds.topic import Topic
from cyclonedds.domain import DomainParticipant
from cyclonedds.builtin import BuiltinDataReader, BuiltinTopicDcpsTopic


@dataclass
@annotate.appendable
@annotate.autoid("hash")
class EmptyType(idl.IdlStruct, typename="test.EmptyType"):
    identifier: types.uint64
    annotate.key("identifier")


# We can't control the finalization order of objects during reference cycle collection.
# Probably issues caused by this:
# 1) Can C functions can be finalized and invalidated before their listener/entity? - SEGFAULT
# 2) Deletion of Listeners before disconnection from the associated entity might be invalid too? - ?
# 3) It might be tempting for app code to do cleanup (dispose instance on a publisher already deleted) during a cycle collection - SEGFAULT
    # Actually I think 3 was the same as 1 - it just happened the dispose call was what tirggered the deleted listener.

# We currently call dds_delete ONLY on Entities which are NOT in a reference cycle (due to checking presence in a WeakValueDictionary).
# Probable issues caused by this:
# 1) We can destruct Listeners and associated functions wihtout ever deleting the associated Entities. - SEGFAULT
# 2) Memory/resource leaks for apps that create/delete Pubs/Subs over time (provided there are ref cycles).

# When removing this WeakValueDictionary check (and therefore calling dds_delete too often, but reliable) the standard test suite hangs.
# The specific tests that hang seem OK in isolation so tracking this down to a MCVE has proved frustrating.

# Regardless of the presence of reference cycles or the WeakValueDictionary check I have code that hangs (still investigating).


# The test in this file causes a segfault for me, and oddly.. hangs if I remove the gc.collect() call.
def test_topic_creation_force_segfault_via_cycle():
    import gc

    # We hold 2 references here: [participant, reader]
    # The reader holds a reference to: [participant, listener]
    participant = DomainParticipant()
    reader = BuiltinDataReader(
        participant,
        BuiltinTopicDcpsTopic,
        listener=Listener(on_data_available=lambda *a: None),
    )

    # If we don't create a topic before deleting the reader the problem doesn't repro for me
    t = Topic(participant, "test", EmptyType)
    t = None

    # Force delete the listener and reader - the reader won't be deleted because this
    # is a reference cycle delete - but the listener and it's functions will be!
    # N.B. `BuiltinDataReader`s have a ref cycle in 0.10.1 - fixed by #134 - however the
    # segfault shouldn't happen regardless.
    reader = None

    # It's unlikely the GC happens to run at this moment so we force it to.
    gc.collect()

    # This segfaults as the reader is still alive according to DDS, but the Listener and
    # it's functions have been destructed.
    t = Topic(participant, "test2", EmptyType)

    # Unreachable for me

# The test in this file causes a segfault for me, and oddly.. hangs if I remove the gc.collect() call.
def test_topic_creation_force_hang_via_cycle():
    import gc

    # We hold 2 references here: [participant, reader]
    # The reader holds a reference to: [participant, listener]
    participant = DomainParticipant()
    reader = BuiltinDataReader(
        participant,
        BuiltinTopicDcpsTopic,
        listener=Listener(on_data_available=lambda *a: None),
    )

    # If we don't create a topic before deleting the reader the problem doesn't repro for me
    t = Topic(participant, "test", EmptyType)
    t = None

    # Force delete the listener and reader - the reader won't be deleted because this
    # is a reference cycle delete - but the listener and it's functions will be!
    # N.B. `BuiltinDataReader`s have a ref cycle in 0.10.1 - fixed by #134 - however the
    # segfault shouldn't happen regardless.
    reader = None

    # This hangs! Something failing to be deleted due to the reference cycle? Or another issue?
    t = Topic(participant, "test2", EmptyType)

    # Unreachable for me

