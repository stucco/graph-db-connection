-- TODO: add return as count of inserted vertices/edges 
BEGIN;   

	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

	-- function to create tsv column for tables which use GIST index
	CREATE OR REPLACE FUNCTION add_tsv(_tableName text) RETURNS void AS $$ 
		DECLARE _tableExists boolean; 
		BEGIN 
			SELECT EXISTS INTO _tableExists (SELECT * FROM information_schema.columns WHERE table_name = _tableName AND column_name='tsv'); 
			IF _tableExists = 'f' 
			THEN 
				EXECUTE format('ALTER TABLE %s ADD COLUMN tsv tsvector;', _tableName); 
				EXECUTE format('UPDATE %s SET tsv = to_tsvector(''english'', coalesce(array_to_string(alias, '' ''), '' ''));', _tableName); 
				EXECUTE format('CREATE INDEX ON %s USING gist(tsv);', _tableName); 
			END IF; 
		END; 
	$$ language plpgsql;

	-- function to create trigger to combine name and alias for tables containing both columns
	CREATE OR REPLACE FUNCTION search_trigger() RETURNS TRIGGER AS $search_trigger$  
		BEGIN  
			new.tsv := to_tsvector('english', coalesce(array_to_string(new.alias, ' '), ' ') || ' ' || coalesce(new.name));  
			return new;  
		END;  
	$search_trigger$ LANGUAGE plpgsql; 

	-- function to add search_trigger to tables containing name and alias columns to execute on all inserts, updates, and delites
	CREATE OR REPLACE FUNCTION add_trigger(_tableName text) RETURNS void as $$  
		DECLARE _count int;  
		BEGIN  
			SELECT count(event_object_table) into _count FROM information_schema.triggers WHERE trigger_name = 'tsvectorupdate' AND event_object_table = _tableName;  
			IF _count = 0  
				THEN CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE ON _tableName FOR EACH ROW EXECUTE PROCEDURE search_trigger();  
			END IF; 
		END;  
	$$ language plpgsql; 
 
	-- creates (if it it null) uuid for vertices during insertion
	CREATE OR REPLACE FUNCTION get_id(_id text) returns text as $$  
		BEGIN  
			IF _id IS NULL  
				THEN return uuid_generate_v4();  
			ELSE  
				return _id;  
			END IF;  
		END; 
	$$ language plpgsql; 

	-- function to set UNIQUE constraint on table column if it does not exists
	CREATE OR REPLACE FUNCTION add_unique_constraint(_tableName text, _indexName text, _columns text) RETURNS void AS $$  
		DECLARE _constraintExists boolean;  
		BEGIN  
			_indexName = lower(_indexName);
			SELECT EXISTS (SELECT * FROM pg_constraint WHERE conname = _indexName) INTO _constraintExists; 
			IF _constraintExists = 'f'  
				THEN  
					EXECUTE 'ALTER TABLE ' || _tableName || ' ADD CONSTRAINT ' || _indexName || ' UNIQUE (' || _columns || ')';  
			END IF;  
		END;  
	$$ language plpgsql; 

	CREATE OR REPLACE FUNCTION merge_graph(_tempTable text) RETURNS void AS $$  
		BEGIN  
			EXECUTE 'CREATE TABLE if not exists ' || _tempTable || '_duplicates (vertex_id text, duplicate_id text);';

			PERFORM merge_vertices(_tempTable);
			PERFORM merge_edges(_tempTable);

			EXECUTE 'DROP TABLE ' || _tempTable || '_duplicates;';
		END;  
	$$ language plpgsql; 

	CREATE OR REPLACE FUNCTION merge_edges(_tempTable text) RETURNS void AS $$  
		DECLARE
			_details text;
			_message text;
			_hint text;
		BEGIN 
			EXECUTE
			'WITH new_edges AS (
				SELECT 
					e->>''outVertID'' AS outVertID,  
					e->>''inVertID'' AS inVertID,  
					e->>''outVertTable'' AS outVertTable,  
					e->>''inVertTable'' AS inVertTable,  
					e->>''relation'' AS relation  
				FROM (
					SELECT json_array_elements((graph->>''edges'')::json)  
					AS e  
					FROM ' || _tempTable || '
				)  
				AS j
			) 
			SELECT 
				insert_edge( 
					outVertID,  
					(select duplicate_id from ' || _tempTable || '_duplicates where vertex_id = outVertID limit 1),  
					inVertID,  
					(select duplicate_id from ' || _tempTable || '_duplicates where vertex_id = inVertID limit 1),  
					outVertTable,  
					inVertTable,  
					relation 
				)
			FROM new_edges 
			GROUP BY outVertID, inVertID, outVertTable, inVertTable, relation';

		EXCEPTION WHEN OTHERS THEN 
			GET STACKED DIAGNOSTICS _details = PG_EXCEPTION_DETAIL, _message = MESSAGE_TEXT, _hint = PG_EXCEPTION_HINT;
            RAISE NOTICE 'PG_EXCEPTION_DETAIL: %', _details;
            RAISE NOTICE 'MESSAGE_TEXT: %', _message;
            RAISE NOTICE 'PG_EXCEPTION_HINT: %', _hint;
		END;  
	$$ language plpgsql;  

	-- function to parse edge and insert it into table
	-- function to parse vertices is more complicated and depends on config file, so InitializePostgresqlDB is building this function for vertices
	-- CREATE OR REPLACE FUNCTION insert_edge(_outVertID text, _inVertID text, _outVertTable text, _inVertTable text, _relation text, _vertex_id text, _duplicate_id text) RETURNS void AS $$   
	CREATE OR REPLACE FUNCTION insert_edge(_outVertID text, _outVertDuplicateID text, _inVertID text, _inVertDuplicateID text, _outVertTable text, _inVertTable text, _relation text) RETURNS void AS $$   
		DECLARE
			_details text;
			_message text;
			_hint text;
		BEGIN    
			IF _inVertDuplicateID IS NOT NULL THEN
				PERFORM update_xml(_outVertID, _outVertTable, _inVertID, _inVertDuplicateID);
				_inVertID := _inVertDuplicateID;
			END IF;

			IF _outVertDuplicateID IS NOT NULL then 
				_outVertID := _outVertDuplicateID;
			END IF;

			INSERT INTO Edges (outVertID, inVertID, outVertTable, inVertTable, relation)  
			VALUES (  
				_outVertID,  
				_inVertID,  
				_outVertTable,  
				_inVertTable,  
				_relation)  
			ON CONFLICT ("relation", "outvertid", "invertid") 
			DO NOTHING;  
		EXCEPTION WHEN OTHERS THEN 
			GET STACKED DIAGNOSTICS _details = PG_EXCEPTION_DETAIL, _message = MESSAGE_TEXT, _hint = PG_EXCEPTION_HINT;
            RAISE NOTICE 'PG_EXCEPTION_DETAIL: %', _details;
            RAISE NOTICE 'MESSAGE_TEXT: %', _message;
            RAISE NOTICE 'PG_EXCEPTION_HINT: %', _hint;
			RAISE NOTICE 'outVertID: %, inVertID: %, outVertTable: %, inVertTable: %, relation: %', _outVertID, _inVertID, _outVertTable, _inVertTable, _relation;
		END; 
	$$ language plpgsql; 

	CREATE OR REPLACE FUNCTION merge_vertices(_tempTable text) RETURNS void AS $$
		BEGIN    
			EXECUTE 
			'WITH duplicates AS (
				SELECT key, insert_vertex(key, value) AS duplicate_id 
				FROM (
					SELECT (json_each(graph.vertices)).key, (json_each(graph.vertices)).value 
					FROM (
						SELECT (graph->>''vertices'')::json AS vertices 
						FROM ' || _tempTable || ') 
					AS graph) 
				AS vertex) 
			INSERT INTO ' || _tempTable || '_duplicates
			SELECT key, duplicate_id
			FROM duplicates 
			WHERE duplicate_id <> key';
		END; 
	$$ language plpgsql;

	-- this function is automatically generated by IntializePostgresqlDB class based on tables.json config file
	-- in this file it is serving as an example
	-- function to parse vertex, search for duplicate based on constraints and insert it into correct table or merge with found duplicate
	/*	
	CREATE OR REPLACE FUNCTION insert_vertex(vertex_id text, vertex json) RETURNS text as $$ 
		DECLARE id text; 
				duplicate boolean;
				related_id text;
  				detail text;
		BEGIN
			CASE vertex->>'vertexType' 
				WHEN 'AddressRange' THEN
					INSERT INTO AddressRange (_id, vertexType, observableType, name, startIP, startIPInt, endIP, endIPInt, description, source, sourceDocument, location)
					VALUES (
						vertex_id,
						'AddressRange',
						vertex->>'observableType',
						vertex->>'name',
						vertex->>'startIP',
						(vertex->>'startIPInt')::bigint,
						vertex->>'endIP',
						(vertex->>'endIPInt')::bigint,
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument',
						vertex->>'location')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(AddressRange.description || EXCLUDED.description))),
						source=(ARRAY(SELECT DISTINCT unnest(AddressRange.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, AddressRange._id),
						modifieddate=now()
					RETURNING AddressRange._id
					INTO id;
					SELECT _id INTO related_id FROM IP WHERE ipInt BETWEEN (vertex->>'startIPInt')::bigint AND (vertex->>'endIPInt')::bigint;
					IF related_id IS NOT NULL THEN
						INSERT INTO Edges (outVertID, inVertID, outVertTable, inVertTable, relation)  
						VALUES (  
							related_id,
							vertex_id,    
							'IP',  
							'AddressRange',  
							'Contained_Within')  
						ON CONFLICT (relation, outVertID, inVertID) 
						DO NOTHING; 
					END IF;
				WHEN 'IP' THEN
					INSERT INTO IP (_id, vertexType, observableType, name, ipInt, description, source, sourceDocument)
					VALUES (
						vertex_id,
						'IP',
						vertex->>'observableType',
						vertex->>'name',
						(vertex->>'ipInt')::bigint,
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(IP.description || EXCLUDED.description))),
						source=(ARRAY(SELECT DISTINCT unnest(IP.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, IP._id),
						modifieddate=now()
					RETURNING IP._id
					INTO id;
					SELECT _id INTO related_id FROM AddressRange WHERE (vertex->>'ipInt')::bigint BETWEEN startIPInt and endIPInt;
					IF related_id IS NOT NULL THEN
						INSERT INTO Edges (outVertID, inVertID, outVertTable, inVertTable, relation)  
						VALUES (  
							vertex_id,
							related_id,  
							'IP',  
							'AddressRange',  
							'Contained_Within')  
						ON CONFLICT (relation, outVertID, inVertID) 
						DO NOTHING; 
					END IF;
				WHEN 'Vulnerability' THEN
					INSERT INTO Vulnerability (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'Vulnerability',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Vulnerability.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(Vulnerability.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(Vulnerability.source || EXCLUDED.source))),
						-- _id=duplicateID(vertex_id, Vulnerability._id),
						modifieddate=now()
					RETURNING Vulnerability._id
					INTO id;
				WHEN 'Exploit_Target' THEN
					INSERT INTO Exploit_Target (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'Exploit_Target',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Exploit_Target.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(Exploit_Target.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(Exploit_Target.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Exploit_Target._id),
						modifieddate=now()
					RETURNING Exploit_Target._id
					INTO id;
				WHEN 'Indicator' THEN
					INSERT INTO Indicator (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'Indicator',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Indicator.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(Indicator.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(Indicator.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Indicator._id),
						modifieddate=now()
					RETURNING Indicator._id
					INTO id;
				WHEN 'Observable' THEN
					INSERT INTO Observable (_id, vertexType, observableType, name, alias, description, source, sourceDocument)
					VALUES (
						vertex_id,
						'Observable',
						vertex->>'observableType',
						vertex->>'name',
						json_to_array((vertex->>'alias')::json),
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name", "observabletype")
					DO UPDATE
					SET
						alias=(ARRAY(SELECT DISTINCT unnest(Observable.alias || EXCLUDED.alias))),
						description=(ARRAY(SELECT DISTINCT unnest(Observable.description || EXCLUDED.description))),
						source=(ARRAY(SELECT DISTINCT unnest(Observable.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Observable._id),
						modifieddate=now()
					WHERE Observable._id IS DISTINCT FROM vertex_id
					RETURNING Observable._id
					INTO id;
				WHEN 'TTP' THEN
					INSERT INTO TTP (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'TTP',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(TTP.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(TTP.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(TTP.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, TTP._id),
						modifieddate=now()
					RETURNING TTP._id
					INTO id;
				WHEN 'Campaign' THEN
					SELECT _id INTO id FROM Campaign WHERE json_to_array((vertex->>'alias')::json) && alias;
					IF id IS NULL THEN 
						INSERT INTO Campaign (_id, vertexType, name, alias, description, shortDescription, source, sourceDocument)
						VALUES (
							vertex_id,
							'Campaign',
							vertex->>'name',
							json_to_array((vertex->>'alias')::json),
							json_to_array((vertex->>'description')::json),
							json_to_array((vertex->>'shortDescription')::json),
							json_to_array((vertex->>'source')::json),
							vertex->>'sourceDocument')
						RETURNING Campaign._id
						INTO id;
					ELSE
						UPDATE Campaign SET
							alias=(ARRAY(SELECT DISTINCT unnest(alias || json_to_array((vertex->>'alias')::json)))),
							description=(ARRAY(SELECT DISTINCT unnest(description || json_to_array((vertex->>'description')::json)))),
							shortDescription=(ARRAY(SELECT DISTINCT unnest(shortDescription || json_to_array((vertex->>'shortDescription')::json)))),
							details=(ARRAY(SELECT DISTINCT unnest(details || json_to_array((vertex->>'details')::json)))),
							source=(ARRAY(SELECT DISTINCT unnest(source || json_to_array((vertex->>'source')::json)))), 
							modifieddate=now()
						WHERE _id=id;
						-- duplicateID(vertex_id, id);
						-- INSERT INTO duplicates VALUES (vertex_id, id);
					END IF;
				WHEN 'Course_Of_Action' THEN
					INSERT INTO Course_Of_Action (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'Course_Of_Action',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Course_Of_Action.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(Course_Of_Action.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(Course_Of_Action.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Course_Of_Action._id),
						modifieddate=now()
					RETURNING Course_Of_Action._id
					INTO id;
				WHEN 'Weakness' THEN
					INSERT INTO Weakness (_id, vertexType, name, description, source, sourceDocument)
					VALUES (
						vertex_id,
						'Weakness',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Weakness.description || EXCLUDED.description))),
						source=(ARRAY(SELECT DISTINCT unnest(Weakness.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Weakness._id),
						modifieddate=now()
					RETURNING Weakness._id
					INTO id;
				WHEN 'Malware' THEN
					-- SELECT EXISTS INTO duplicate (SELECT * FROM Malware WHERE json_to_array((vertex->>'alias')::json) && alias);
					SELECT _id INTO id FROM Malware WHERE json_to_array((vertex->>'alias')::json) && alias;
					IF id IS NULL
						THEN
							INSERT INTO Malware (_id, vertexType, name, alias, description, shortDescription, details, source, sourceDocument)
							VALUES (
								vertex_id,
								'Malware',
								vertex->>'name',
								json_to_array((vertex->>'alias')::json),
								json_to_array((vertex->>'description')::json),
								json_to_array((vertex->>'shortDescription')::json),
								json_to_array((vertex->>'details')::json),
								json_to_array((vertex->>'source')::json),
								(vertex->>'sourceDocument'))
							RETURNING _id
							INTO id;
					ELSE 
						UPDATE Malware SET
							alias=(ARRAY(SELECT DISTINCT unnest(alias || json_to_array((vertex->>'alias')::json)))),
							description=(ARRAY(SELECT DISTINCT unnest(description || json_to_array((vertex->>'description')::json)))),
							shortDescription=(ARRAY(SELECT DISTINCT unnest(shortDescription || json_to_array((vertex->>'shortDescription')::json)))),
							details=(ARRAY(SELECT DISTINCT unnest(details || json_to_array((vertex->>'details')::json)))),
							source=(ARRAY(SELECT DISTINCT unnest(source || json_to_array((vertex->>'source')::json)))), 
							modifieddate=now()
						WHERE _id = id;
						-- INSERT INTO duplicates VALUES (vertex_id, id);
					END IF;
				WHEN 'Exploit' THEN
					INSERT INTO Exploit (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'Exploit',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Exploit.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(Exploit.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(Exploit.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Exploit._id),
						modifieddate=now()
					RETURNING Exploit._id
					INTO id;
				WHEN 'Incident' THEN
					INSERT INTO Incident (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'Incident',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Incident.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(Incident.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(Incident.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Incident._id),
						modifieddate=now()
					RETURNING Incident._id
					INTO id;
				WHEN 'Threat_Actor' THEN
					INSERT INTO Threat_Actor (_id, vertexType, name, description, shortDescription, source, sourceDocument)
					VALUES (
						vertex_id,
						'Threat_Actor',
						vertex->>'name',
						json_to_array((vertex->>'description')::json),
						json_to_array((vertex->>'shortDescription')::json),
						json_to_array((vertex->>'source')::json),
						vertex->>'sourceDocument')
					ON CONFLICT ("name")
					DO UPDATE
					SET
						description=(ARRAY(SELECT DISTINCT unnest(Threat_Actor.description || EXCLUDED.description))),
						shortDescription=(ARRAY(SELECT DISTINCT unnest(Threat_Actor.shortDescription || EXCLUDED.shortDescription))),
						source=(ARRAY(SELECT DISTINCT unnest(Threat_Actor.source || EXCLUDED.source))), 
						-- _id=duplicateID(vertex_id, Threat_Actor._id),
						modifieddate=now()
					RETURNING Threat_Actor._id
					INTO id;
				-- ELSE 
				--	RETURN null; 
				INSERT INTO id_mapping (vertex_id, postgres_id) VALUES (vertex_id, id);
			ELSE 
				RETURN NULL;
			END CASE; 
			RETURN id;
		EXCEPTION WHEN OTHERS THEN 
			GET STACKED DIAGNOSTICS detail = PG_EXCEPTION_DETAIL;
			RAISE NOTICE 'PG_EXCEPTION_DETAIL: %', detail;
			RAISE NOTICE 'VERTEX: %', vertex;
			RETURN NULL;
		END; 
	$$ language plpgsql;
	*/	

	CREATE OR REPLACE FUNCTION json_to_array(_jsonArray json) RETURNS text[] AS $$
		DECLARE _arraySql text[];   
				_i text;
		BEGIN    
			FOR _i IN SELECT * FROM json_array_elements_text(_jsonArray)
			LOOP
				IF _i != 'Malware' THEN
					_arraySql = (_arraySql || _i);
				END IF;
			END LOOP;

			RETURN _arraySql;
		END; 
	$$ language plpgsql;

	-- select insert_vertex(key, value) as duplicate_id from (select (json_each(graph.vertices)).key, (json_each(graph.vertices)).value from (SELECT (graph->>'vertices')::json AS vertices FROM v2579uiud2) AS graph) as vertex;

	CREATE OR REPLACE FUNCTION duplicateID(_vertex_id text, _duplicate_id text) RETURNS text AS $$
		BEGIN    
			INSERT INTO duplicates VALUES (_vertex_id, _duplicate_id);
			
			RETURN _duplicate_id;
		END; 
	$$ language plpgsql;

	-- functions to update stix xml to point to correct entity after duplicate detection; required to build report	
	CREATE OR REPLACE FUNCTION update_xml(_vertex_id text, _tableName text, _from_id text, _to_id text) RETURNS void AS $$
		BEGIN    
			EXECUTE 'UPDATE ' || _tableName || ' SET sourceDocument = replace(sourceDocument, ''' || _from_id ||''', ''' || _to_id || ''') WHERE _id = ''' || _vertex_id || ''';';
		END; 
	$$ language plpgsql;

COMMIT;









