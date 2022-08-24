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


# Creating and destroying objects really quickly seems to reliably reproduce a hang/deadlock
# in the cyclonedds lib (strace showing a bunch of futex stuff, I haven't got a bt)

# This is reproducible using the no_cycle approach, and only reproducible using the
# cycles example if the patch avoiding the check of `WeakValueDictionary` is applied.

# i.e. It must be related to entity lifecycle as without the `WeakValueDictionary` patch
# any objects in reference cycles are not deleted (using `dds_delete`).


def test_build_and_destroy_app_no_cycle():
    participant = DomainParticipant()
    for i in range(1024):
        build_and_destroy_app_no_cycle(participant)


def build_and_destroy_app_no_cycle(participant):
    calls = 0
    def cb(*a):
        nonlocal calls
        calls += 1
    reader = BuiltinDataReader(
        participant,
        BuiltinTopicDcpsTopic,
        listener=Listener(on_data_available=cb, on_sample_lost=cb),
    )

    t = Topic(participant, "test", EmptyType)
    # assert calls == 1




# def test_build_and_destroy_app_cycle():
#     participant = DomainParticipant()
#     for i in range(2000):
#         build_and_destroy_app_cycle(participant)

# def build_and_destroy_app_cycle(participant):
#     import gc

#     class Circles:
#         calls = 0

#         def __init__(self):
#             def cb(*a):
#                 self.calls += 1

#             listener = Listener(
#                 on_data_available=cb,
#                 on_sample_lost=cb,
#             )
#             self.reader = BuiltinDataReader(
#                 participant,
#                 BuiltinTopicDcpsTopic,
#                 listener=listener,
#             )
#             self.listener = listener

#             # Ref cycles abound:
#             # self -> reader -> listener -> self

#     c = Circles()

#     # Put the topic on the object so it's GCd around the same time
#     c.t = Topic(participant, "test", EmptyType)
