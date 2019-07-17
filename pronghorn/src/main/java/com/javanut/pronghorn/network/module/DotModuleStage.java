package com.javanut.pronghorn.network.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.network.config.HTTPContentType;
import com.javanut.pronghorn.network.config.HTTPContentTypeDefaults;
import com.javanut.pronghorn.network.config.HTTPHeader;
import com.javanut.pronghorn.network.config.HTTPRevision;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerb;
import com.javanut.pronghorn.network.config.HTTPVerbDefaults;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.util.AppendableByteWriter;

/**
 * Rest service stage which responds with the dot file needed to display the telemetry.
 * This dot file is a snapshot of the system representing its state within the last 40 ms.
 * @param <T>
 * @param <R>
 * @param <V>
 * @param <H>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class DotModuleStage<   T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractAppendablePayloadResponseStage<T,R,V,H> {

	private static final Logger logger = LoggerFactory.getLogger(DotModuleStage.class);
	private final String graphName;
	
    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, PipeMonitorCollectorStage monitor,
    		                  Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs, HTTPSpecification<?, ?, ?, ?> httpSpec) {

    	return new DotModuleStage(graphManager, inputs, outputs, httpSpec, monitor);
    }
    
    public static DotModuleStage<?, ?, ?, ?> newInstance(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output, HTTPSpecification<?, ?, ?, ?> httpSpec) {
    	PipeMonitorCollectorStage monitor = PipeMonitorCollectorStage.attach(graphManager);		
        return new DotModuleStage(graphManager, new Pipe[]{input}, new Pipe[]{output}, httpSpec, monitor);
    }
	
    private final PipeMonitorCollectorStage monitor;

	/**
	 *
	 * @param graphManager
	 * @param inputs _in_ Pipe containing request for generated .dot file.
	 * @param outputs _out_ Pipe that will contain HTTP response containing newly generated .dot file.
	 * @param httpSpec
	 * @param monitor
	 */
	public DotModuleStage(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputs, 
			Pipe<ServerResponseSchema>[] outputs, 
			HTTPSpecification httpSpec, PipeMonitorCollectorStage monitor) {
		super(graphManager, inputs, outputs, httpSpec, dotEstimate(graphManager));
		this.monitor = monitor;
		this.graphName = "AGraph";

		
		if (inputs.length>1) {
			GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		}
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, 100_000_000L, this);

	}
	
	private static int dotEstimate(GraphManager graphManager) {
		
		return (300*GraphManager.countStages(graphManager))+
		       (400*GraphManager.allPipes(graphManager).length);

	}
	

	@Override
	protected boolean payload(AppendableByteWriter<?> payload, 
			                 GraphManager gm, 
			                 ChannelReader params,
			                 HTTPVerbDefaults verb) {
		
		//TODO: we need a better way to support this..
		
		
		//logger.info("begin building requested graph");
		monitor.writeAsDot(gm, graphName, payload);
		
		//logger.info("finished requested dot");
		return true;//return false if we are not able to write it all...
	}
	
	@Override
	public HTTPContentType contentType() {
		return HTTPContentTypeDefaults.DOT;
	}

}
