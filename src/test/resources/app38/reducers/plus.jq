import "../common/op.jq" as op;

.state + {
             "value" : .state.value | op::incr
         }
