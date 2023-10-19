set(SOURCES rwbench.cc)

add_library(rwbench STATIC ${SOURCES})

set_target_properties(rwbench PROPERTIES LINKER_LANGUAGE CXX)

# target_link_libraries(rwbench rwlock)