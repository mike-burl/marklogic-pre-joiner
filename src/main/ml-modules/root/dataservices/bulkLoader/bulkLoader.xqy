xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $session as document-node()? external;
declare variable $endpointState as document-node()? external;
declare variable $workUnit as document-node()? external;
declare variable $input as document-node()* external;

let $primaryName := $workUnit/entity/name/fn:string()
let $primaryKeyElement := $workUnit/entity/primaryKey/fn:string()
let $uriPrefix := $workUnit/entity/uriPrefix/fn:string()
let $uriSuffix := $workUnit/entity/uriSuffix/fn:string()
let $collections := $workUnit/entity/collections/collection/fn:string()

let $_ := for $doc in $input
    let $primaryID := 
      fn:normalize-space($doc/envelope/element()[local-name() = fn:concat($primaryName, '_Collection')]/*[1]/element()[local-name() = $primaryKeyElement]/fn:string())
    let $uri := fn:concat($uriPrefix, $primaryID, $uriSuffix)
    return xdmp:document-insert($uri, $doc, map:map() => map:with("collections", ($collections)))
    
return element status {"Success"}