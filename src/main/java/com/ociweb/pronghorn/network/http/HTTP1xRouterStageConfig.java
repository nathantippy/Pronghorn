package com.ociweb.pronghorn.network.http;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.ServerConnectionStruct;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructType;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xRouterStageConfig<T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> implements RouterStageConfig {
	
	public static final Logger logger = LoggerFactory.getLogger(HTTP1xRouterStageConfig.class);

	public final HTTPSpecification<T,R,V,H> httpSpec;
	
    public final TrieParser urlMap;
    public final TrieParser verbMap;
    public final TrieParser revisionMap;
      
    private final int defaultLength = 4;
    
    private TrieParser[] headersParser = new TrieParser[4];    
    private JSONExtractorCompleted[] requestJSONExtractorForPath = new JSONExtractorCompleted[defaultLength];    
    private FieldExtractionDefinitions[] pathToRoute = new FieldExtractionDefinitions[defaultLength];
    
	private int routeCount = 0;
	private AtomicInteger pathCount = new AtomicInteger();
 
    
    final int UNMAPPED_ROUTE =   (1<<((32-2)-HTTPVerb.BITS))-1;//a large constant which fits in the verb field
    public final int UNMAPPED_STRUCT; 
    final TrieParser unmappedHeaders;
    public final long unmappedPathField;
    public int[] unmappedIndexPos;

	private IntHashTable routeIdTable = new IntHashTable(3);
	
    
    private URLTemplateParser routeParser;
    private final ServerConnectionStruct conStruct;
	
	private final TrieParserReader localReader = new TrieParserReader(true);


	public int totalSizeOfIndexes(int structId) {
		return conStruct.registry.totalSizeOfIndexes(structId);
	}
	
	public <T extends Object> T getAssociatedObject(long field) {
		return conStruct.registry.getAssociatedObject(field);
	}
	
	public HTTP1xRouterStageConfig(HTTPSpecification<T,R,V,H> httpSpec, 
									ServerConnectionStruct conStruct) {
		this.httpSpec = httpSpec;
		this.conStruct = conStruct;
        this.revisionMap = new TrieParser(256,true); //avoid deep check        
        //Load the supported HTTP revisions
        R[] revs = (R[])httpSpec.supportedHTTPRevisions.getEnumConstants();
        if (revs != null) {
	        int z = revs.length;               
	        while (--z >= 0) {
	        	revisionMap.setUTF8Value(revs[z].getKey(), "\r\n", revs[z].ordinal());
	            revisionMap.setUTF8Value(revs[z].getKey(), "\n", revs[z].ordinal()); //\n must be last because we prefer to have it pick \r\n          
	        }
        }
        
        this.verbMap = new TrieParser(256,false);//does deep check
        //logger.info("building verb map");
        //Load the supported HTTP verbs
        V[] verbs = (V[])httpSpec.supportedHTTPVerbs.getEnumConstants();
        if (verbs != null) {
	        int y = verbs.length;
	        assert(verbs.length>=1) : "only found "+verbs.length+" defined";
	        while (--y >= 0) {
	        	//logger.info("add verb {}",verbs[y].getKey());
	            verbMap.setUTF8Value(verbs[y].getKey()," ", verbs[y].ordinal());           
	        }
        }


        //unknowns are the least important and must be added last 
        this.urlMap = new TrieParser(512,2,false //never skip deep check so we can return 404 for all "unknowns"
        	 	                   ,true,true);
        
		String constantUnknownRoute = "${path}";//do not modify
		int routeId = UNMAPPED_ROUTE;//routeCount can not be inc due to our using it to know if there are valid routes.
		int pathId = UNMAPPED_ROUTE;
				
		int structId = HTTPUtil.newHTTPStruct(conStruct.registry);
		unmappedPathField = conStruct.registry.growStruct(structId,StructType.Text,0,"path".getBytes());				
		
		unmappedIndexPos = new int[] {StructRegistry.FIELD_MASK&(int)unmappedPathField};
		
		routeParser().addPath(constantUnknownRoute, routeId, pathId, structId);
		UNMAPPED_STRUCT = structId;
		
		unmappedHeaders = HTTPUtil.buildHeaderParser(
				conStruct.registry, 
    			structId,
    			HTTPHeaderDefaults.CONTENT_LENGTH,
    			HTTPHeaderDefaults.TRANSFER_ENCODING,
    			HTTPHeaderDefaults.CONNECTION
			);
		
	}

		
	public void debugURLMap() {
		
		try {
			urlMap.toDOTFile(File.createTempFile("debugTrie", ".dot"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}



	private URLTemplateParser routeParser() {
		//Many projects do not need this so do not build..
		if (routeParser==null) {
	        boolean trustText = false; 
			routeParser = new URLTemplateParser(urlMap, trustText);
		}
		return routeParser;
	}

	public void storeRouteHeaders(int routeId, TrieParser headerParser) {
		if (routeId>=headersParser.length) {
			int i = headersParser.length;
			TrieParser[] newArray = new TrieParser[i*2];
			System.arraycopy(headersParser, 0, newArray, 0, i);
			headersParser = newArray;
		}
		headersParser[routeId]=headerParser;
	}
	
	void storeRequestExtractionParsers(int pathIdx, FieldExtractionDefinitions route) {
		
		//////////store for lookup by path
		if (pathIdx>=pathToRoute.length) {
			int i = pathToRoute.length;
			FieldExtractionDefinitions[] newArray = new FieldExtractionDefinitions[i*2]; //only grows on startup as needed
			System.arraycopy(pathToRoute, 0, newArray, 0, i);
			pathToRoute = newArray;
		}
		pathToRoute[pathIdx]=route;	

		//we have 1 pipe per composite route so nothing gets stuck and
		//we have max visibility into the traffic by route type.
		//any behavior can process multiple routes but it comes in as multiple pipes.
		//each pipe can however send multiple different routes if needed since each 
		//message contains its own structId
	}

	void storeRequestedJSONMapping(int pathId, JSONExtractorCompleted extractor) {
		
		if (pathId>=requestJSONExtractorForPath.length) {
			int i = requestJSONExtractorForPath.length;
			JSONExtractorCompleted[] newArray = new JSONExtractorCompleted[i*2]; //only grows on startup as needed
			System.arraycopy(requestJSONExtractorForPath, 0, newArray, 0, i);
			requestJSONExtractorForPath = newArray;
		}
		requestJSONExtractorForPath[pathId] = extractor;
	}
	
	public int totalPathsCount() {
		return pathCount.get();
	}

	public int getRouteIdForPathId(int pathId) {
		return (pathId != UNMAPPED_ROUTE) ? extractionParser(pathId).routeId : -1;
	}

	//only needed on startup, ok to be linear search
	public int getStructIdForRouteId(final int routeId) {
		
		int result = -1;
		if (routeId != UNMAPPED_ROUTE) {
			int i = pathToRoute.length;
			while (--i>=0) {
				
				if ((pathToRoute[i]!=null) && (routeId == pathToRoute[i].routeId)) {
					if (result==-1) {
						result = pathToRoute[i].structId;
					} else {
						assert(result == pathToRoute[i].structId) : "route may only have 1 structure, found more";					
					}
				}
			}
			if (-1 == result) {
				throw new UnsupportedOperationException("Unable to find routeId "+routeId);			
			}
		} else {
			result = UNMAPPED_STRUCT;
		}
		return result;
	}
	
	public FieldExtractionDefinitions extractionParser(int pathId) {
		return pathToRoute[pathId];
	}
	
	public TrieParser headerParserRouteId(int routeId) {
		return headersParser[routeId];		
	}

	public JSONExtractorCompleted JSONExtractor(int routeId) {
		return routeId<requestJSONExtractorForPath.length ? requestJSONExtractorForPath[routeId] : null;
	}

    @Override
	public HTTPSpecification httpSpec() {
		return httpSpec;
	}

    public int headerId(byte[] h) {
    	return httpSpec.headerId(h, localReader);
    }
    

	public CompositeRoute registerCompositeRoute(HTTPHeader ... headers) {

		return new CompositeRouteImpl(conStruct, this, null, routeParser(), headers, routeCount++, pathCount);
	}


	public CompositeRoute registerCompositeRoute(JSONExtractorCompleted extractor, HTTPHeader ... headers) {

		return new CompositeRouteImpl(conStruct, this, extractor, routeParser(), headers, routeCount++, pathCount);
	}

	public boolean appendPipeIdMappingForAllGroupIds(
            Pipe<HTTPRequestSchema> pipe, 
            int p, 
            ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes) {
		
			assert(null!=collectedHTTPRequstPipes);
			boolean added = false;
			int i = routeCount;
			if (i==0) {
				added  = true;
				collectedHTTPRequstPipes[p][0].add(pipe); //ALL DEFAULT IN A SINGLE Path
			} else {
				while (--i>=0) {
					added  = true;
					if (null != pathToRoute[i] 
						&& UNMAPPED_ROUTE!=pathToRoute[i].pathId
					   ) {
						assert(null != collectedHTTPRequstPipes[p][pathToRoute[i].pathId]);
						collectedHTTPRequstPipes[p][pathToRoute[i].routeId].add(pipe);
					}
				}
			}
			return added;
	}
	
	public boolean appendPipeIdMappingForIncludedGroupIds(
			                      Pipe<HTTPRequestSchema> pipe, 
			                      int track, 
			                      ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes,
			                      int ... routeId) {
		boolean added = false;
		int i = pathToRoute.length;
		while (--i>=0) {
			if (null!=pathToRoute[i]) {
				if (contains(routeId, pathToRoute[i].routeId)) {	
					added = true;
				    collectedHTTPRequstPipes[track][pathToRoute[i].routeId].add(pipe);	
				}
			}
		}
			
		return added;
	}
	
	public boolean appendPipeIdMappingForExcludedGroupIds(
            Pipe<HTTPRequestSchema> pipe, 
            int track, 
            ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes,
            int ... groupsIds) {
			boolean added = false;
			int i = pathToRoute.length;
			while (--i>=0) {
				if (null!=pathToRoute[i]) {
					if (!contains(groupsIds, pathToRoute[i].routeId)) {			
						added = true;
						collectedHTTPRequstPipes[track][pathToRoute[i].routeId].add(pipe);	
					}
				}
			}
			return added;
	}

	private boolean contains(int[] groupsIds, int groupId) {
		int i = groupsIds.length;
		while (--i>=0) {
			if (groupId == groupsIds[i]) {
				return true;
			}
		}
		return false;
	}

	public int[] paramIndexArray(int pathId) {
		return pathToRoute[pathId].paramIndexArray();
	}

	public int totalRoutesCount() {
		return routeCount;
	}

	public int lookupRouteIdByIdentity(Object associatedObject) {
		
		final int hash = associatedObject.hashCode();
		final int idx = IntHashTable.getItem(routeIdTable, hash);
		if (0==idx) {
			if (!IntHashTable.hasItem(routeIdTable, hash)) {
				throw new UnsupportedOperationException("Object not found: "+associatedObject);			
			}
		}
		return idx;
	}

	public void registerRouteAssociation(int routeId, Object associatedObject) {

		int key = associatedObject.hashCode();
	
		assert(!IntHashTable.hasItem(routeIdTable, key)) : "These objects are too similar or was attached twice, Hash must be unique. Choose different objects";
		if (IntHashTable.hasItem(routeIdTable, key)) {
			logger.warn("Unable to add object {} as an association, Another object with an identical Hash is already held. Try a different object.", associatedObject);		
			return;
		}
		if (!IntHashTable.setItem(routeIdTable, key, routeId)) {
			routeIdTable = IntHashTable.doubleSize(routeIdTable);			
			if (!IntHashTable.setItem(routeIdTable, key, routeId)) {
				throw new RuntimeException("internal error");
			};
		}		
	}

}
