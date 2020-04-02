grammar Query;

query: entity qline EOF ;

entity: (ID '.')* NAME ;

qline: filter? sort? (limit page?)? select ;

  filter: '|' predicate (logic predicate)* '|' ;

    predicate: path oper value ;

    path: ID ('.' ID)* ;

    value: (INT | STRING) ;

  sort: 'sort' (ID order)+;

  limit: 'limit' INT ;

  page: 'page' INT;

  select: '{' fields (',' relation)* '}' ;

    fields: ALL | (field (',' field)*) ;

    field: ID ;

    relation: ID qline ;

order: ('asc' | 'desc') ;

logic: ('or' | 'and') ;

oper: ('==' | '!=' | '>' | '<' | '<=' | '>=' | 'in') ;

NAME: UPPER ID ;
ID: LOWER (NUMBER | LOWER | UPPER)* ;
INT: NUMBER+ ;

NUMBER: '0'..'9' ;
LOWER: 'a'..'z' ;
UPPER: 'A'..'Z' ;

ALL: '*' ;
STRING : '"' (' '..'~')* '"' ;

WS: [ \t\r\n\f]+ -> skip ;