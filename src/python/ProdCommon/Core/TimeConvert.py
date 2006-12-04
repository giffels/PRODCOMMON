import math

import logging

from ProdCommon.Core.ProdException import ProdException

def seconds2H_M_S(remainingSeconds):

    try:
        if remainingSeconds<0:
            raise ProdException("seconds for conversion should be larger than 1",1007)

        secondsInHours=3600
        secondsInMinutes=60
    
        hours=int(math.floor(float(remainingSeconds)/float(secondsInHours)))
        remainingSeconds=remainingSeconds-secondsInHours*hours
        minutes=int(math.floor(float(remainingSeconds)/float(secondsInMinutes)))
        seconds=remainingSeconds-secondsInMinutes*minutes
        timeFormat=str(hours)+":"+str(minutes)+":"+str(seconds)
        return timeFormat
    except Exception,ex:
        raise ProdException("Error converting seconds to H:M:S format: "+str(ex),1008)
