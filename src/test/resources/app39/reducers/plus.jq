import "../common/incr.jq" as op;

.state + {
             "value" : .state.value | op::incr
         }
