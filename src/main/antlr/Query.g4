grammar Query;

prog: (expr)* ;

expr: expr ('*'|'/') expr
  | expr ('+'|'-') expr
  | INT
  | '(' expr ')'
;

INT: [0-9]+ ;

WS: [ \t\r\n\f]+ -> skip ;