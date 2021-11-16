option (USE_UNWIND "Enable libunwind (better stacktraces)" ON)
option (USE_INTERNAL_MEMCPY "Use internal libunwind" OFF)

if (USE_UNWIND)
    if (USE_INTERNAL_MEMCPY)
        add_subdirectory(contrib/libunwind-cmake)
        set (UNWIND_LIBRARIES unwind)
    else()
        find_library(UNWIND unwind REQUIRED)
    endif()

    set (EXCEPTION_HANDLING_LIBRARY ${UNWIND_LIBRARIES})
    message (STATUS "Using libunwind: ${UNWIND_LIBRARIES}")
else ()
    set (EXCEPTION_HANDLING_LIBRARY gcc_eh)
endif ()

message (STATUS "Using exception handler: ${EXCEPTION_HANDLING_LIBRARY}")
