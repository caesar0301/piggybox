package com.piggybox.omnilab.aem;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.Period;

import com.google.common.net.InternetDomainName;
import com.piggybox.utils.tree.TreeNode;

/**
 * The pig UDF implementing the AID algorithm of Activity-Entity Model.
 * Input: a bag of tuples (HttpRequestStartTime, HttpRequestEndTime, URL, Referrer, ContentType ..)
 * Return: a bag of tuples, each tuple being appended by an activity ID (UUID);
 * 
 * @author chenxm
 *
 */
public class DetectActivity extends AccumulatorEvalFunc<DataBag>{
	private double readingTime;
	private AEM aemModel = null;
	private DataBag outputBag = null;
	public Log myLogger = this.getLogger();
	
	public DetectActivity(){
		this("2s");
	}
	
	public DetectActivity(String timeSpec){
		Period p = new Period("PT" + timeSpec.toUpperCase());
	    this.readingTime = p.toStandardSeconds().getSeconds();
		cleanup();
	}
	
	@Override
	public void accumulate(Tuple b) throws ExecException {
		cleanup();
		for ( Tuple t : (DataBag) b.get(0) ){
			Entity newEntity = new Entity(t);
			boolean okToDump = false;
			okToDump = aemModel.addEntityToModel(newEntity);
			int actCnt = aemModel.size();
			if (okToDump && actCnt > 0){
				dumpActivitiesToBag(0, actCnt-1); // leave the last activity to add new entities.
			}
			//this.reporter.progress(); // Disable to pass mvn test
		}
	}

	@Override
	public void cleanup() {
		this.outputBag = BagFactory.getInstance().newDefaultBag();
		this.aemModel = new AEM(readingTime, this.myLogger); // set to 2.5s
	}

	@Override
	public DataBag getValue() {
		dumpActivitiesToBag(0, aemModel.size()); // dump all activities.
		return this.outputBag;
	}
	
	/**
	 * Dump activities from startIndex, inclusive, to endIndex, exclusive to the outputBag.
	 * @param startIndex
	 * @param endIndex
	 */
	private void dumpActivitiesToBag(int startIndex, int endIndex){
		for ( Activity act : aemModel.getActivities(startIndex, endIndex)){
			for ( Entity entity : act.getAllEntities() ){
				Tuple newT = TupleFactory.getInstance().newTuple(entity.getTuple().getAll());
				newT.append(act.getID());
				this.outputBag.add(newT);
			}
		}
		aemModel.removeActivities(startIndex, endIndex);
	}

	/**
	 * The output schema of AEM UDF.
	 * Bag in bag out. But the output bag elements are appended by an activityID.
	 */
	@Override
	public Schema outputSchema(Schema input){
		try {
			Schema.FieldSchema inputFieldSchema = input.getField(0);
			if (inputFieldSchema.type != DataType.BAG){
				throw new RuntimeException("Expected a BAG as input");
			}
			Schema inputBagSchema = inputFieldSchema.schema;
			if (inputBagSchema.getField(0).type != DataType.TUPLE){
				throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
	                                             DataType.findTypeName(inputBagSchema.getField(0).type)));
			}
			Schema inputTupleSchema = inputBagSchema.getField(0).schema;
			if (inputTupleSchema.getField(0).type != DataType.DOUBLE){
				throw new RuntimeException(String.format("Expected first element of tuple to be a DOUBLE, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(1).type != DataType.DOUBLE){
				throw new RuntimeException(String.format("Expected first element of tuple to be a DOUBLE, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(2).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(3).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(4).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(5).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			Schema outputTupleSchema = inputTupleSchema.clone();
			outputTupleSchema.add(new Schema.FieldSchema("activity_id", DataType.CHARARRAY));
			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
	                                           outputTupleSchema,
	                                           DataType.BAG));
		}catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}


/**
 * The Activity-Entity Model of mobile traffic.
 * @author chenxm
 * 
 * Implementation philosophy (Activity and Entity):
 * I utilize the tree data structure contributed by Yifan Peng (github @yfpeng) as
 * the underlying structure of activities and entities.
 * In general, an activity may contains multiple entities while only one activity
 * does an entity link to.
 * You can imagine this as a two-layer graph: 
 * all entities are planted on such an upper layer individually that they do not know each other;
 * on the lower layer, one TreeNode object is created for an entity (with bidirectional pointers) and linked each other (tree structure).
 * An activity only remember the root TreeNode and its entities are handled by the tree structure.
 * So you should make sure the tree has given node before performing processing.
 */
class AEM{
	// Classified entity types
	public static final int TYPE_UNCL = 9;	// unclassified
	public static final int TYPE_CONJ = 0; // conjunction
	public static final int TYPE_PRLL = 1; // parallel
	public static final int TYPE_RLYD = 2; // relayed
	public static final int TYPE_SRAL = 3; // serial
	// AEM configurations
	private static final double CONJ_ST_DIFF = 0.5; //# sec, |ts1-ts2| < 0.5
	private static final double CONF_ET_PCRT = 0.1; //# 10%, |te1-te2|/min(t1,t2) < 0.1
	private static final double RLYD_TDIFF1 = 0;	//# ts2-te1 >= 0
	private static final double RLYD_TDIFF2 = 0.5;	//# sec, ts2-te1 < 0.1
	private static final double SRAL_TDIFF1 = 0;	//# sec, ts2-te1 > 0
	private static final double SRAL_TDIFF2 = 8;	//# sec, ts2-te1 <= 10
	private static final double PRLL_OL = 0;		//# sec, overlap
	private static final double PAGE_FAT = 5;		//# Number of embedded entities of fat page
	private static final double PAGE_SLIM = 2;		//# Number of embedded entities of slim page
	private static final double READING_TIME_DEFAULT = 2;	//# sec, user reading time
	private static double READING_TIME;
	
	private static List<Activity> activities = new LinkedList<Activity>();
	private static Entity lastEntity = null;
	private Log logger = null;
	
	public AEM(){
		this(READING_TIME_DEFAULT, null);
	}
	
	public AEM(double readingTime, Log logger){
		AEM.READING_TIME = readingTime;
		this.logger = logger;
	}
	
	public int size(){
		return AEM.activities.size();
	}
	
	/**
	 * Add a given entity to AEM model correctly.
	 * @param newEntity
	 * @throws ExecException 
	 * @throws Exception 
	 */
	public boolean addEntityToModel(Entity newEntity) throws ExecException{
		boolean createNew = false;
		boolean linked2activity = false;
		boolean dumpModelToBag = false; // indicate if the intermediate data are available to dump.
		
		if ( lastEntity == null ) createNew = true;
		else{
			newEntity.aemLastType = classify(lastEntity, newEntity);
			if (newEntity.aemLastType == AEM.TYPE_UNCL){ // 10s is involved to separate different activities forcedly.
				createNew = true;
				dumpModelToBag = true;
			}else{
				if ( newEntity.referrer != null ){ // with referrer
					Activity act = null;
					Entity removedEntity = null;
					for ( int i = activities.size()-1; i >= 0; i--){ // reversed order
						act = activities.get(i);
						Entity refEntity = act.findReferrerEntity(newEntity);
						if ( refEntity != null ){ // referrer found
							newEntity.aemLastType = classify(lastEntity, newEntity);
							newEntity.aemPredType = classify(refEntity, newEntity);
							act.addEntity(newEntity, refEntity);
							linked2activity = true;
							// Check if cut the activity
							// TODO: for requests generated by redirection, the original URL should also be included
							// in current activity.
							int pch = refEntity.getChildNum(); // child number
							boolean isPageBase = refEntity.isWebPageBase(); // if it is a web page base
							boolean isRefRoot = (act.hasRoot(refEntity) || refEntity.hasFakeReferrer);
							if (!isRefRoot && isPageBase && pch > PAGE_FAT ){
								removedEntity = refEntity;
							}
							if (!isRefRoot && newEntity.aemLastType==TYPE_SRAL && isPageBase && pch>PAGE_SLIM){ // removed bug
								removedEntity = refEntity;
							}
							if (!isRefRoot && newEntity.aemPredType==TYPE_UNCL && isPageBase && pch>PAGE_SLIM){
								removedEntity = refEntity;
							}
							break;
						}
					}
					// Remove cutEntities as new activities
					if ( removedEntity != null ) {
						for ( int j = activities.size()-1; j >= 0; j--){ // reversed order
							Activity a = activities.get(j);
							if ( a.contains(removedEntity)){
								a.removeEntity(removedEntity);
								Activity newA = new Activity(removedEntity);
								activities.add(newA);
								logger.warn("New activity. Model size: " + size());
								break;
							}
						}
					}
				} else { //without referrer
					String sdm1 = getTopPrivateDomain(lastEntity.url);
					String sdm2 = getTopPrivateDomain(newEntity.url);
					if ( newEntity.aemLastType != AEM.TYPE_SRAL || (Math.abs(lastEntity.overlap(newEntity)) < AEM.READING_TIME &&
							sdm1 != null && sdm2!=null && sdm1.equals(sdm2))) {
						// create a fake link to the preceding
						for ( int i = activities.size()-1; i >= 0; i--){ // reversed order
							Activity act = activities.get(i);
							if ( act.contains(lastEntity)){
								// add e to this activity
								newEntity.aemPredType = newEntity.aemLastType;
								newEntity.hasFakeReferrer = true;
								linked2activity = true;
								act.addEntity(newEntity, lastEntity);
								break;
							}
						}
					} else {
						createNew = true;
					}
				}
			}
		}
		if ( createNew || !linked2activity ){
			Activity newActivity = new Activity();
			String ref = newEntity.referrer;
			if ( ref != null ){
				Entity dummyEntity = new Entity(newEntity.start, null, newEntity.referrer, null, null, null, null);
				dummyEntity.isDummy = true; // make sure isDummy set
				newActivity.addEntity(dummyEntity, null);
				newActivity.addEntity(newEntity, dummyEntity);
			} else
				newActivity.addEntity(newEntity, null);
			AEM.activities.add(newActivity);
		}
		lastEntity = newEntity;
		return dumpModelToBag;
	}
	
	/**
	 * Get activities from startIndex to endIndex at appended order.
	 * @return
	 */
	public List<Activity> getActivities(int startIndex, int endIndex){
		return AEM.activities.subList(startIndex, endIndex);
	}
	
	/**
	 * Remove activities from startIndex to endIndex at appended order.
	 */
	public void removeActivities(int startIndex, int endIndex){
		AEM.activities.subList(startIndex, endIndex).clear();
	}
	
	/**
	 * Classify the relationship between entities as one of declairing types in AEM.
	 * @param e1
	 * @param e2
	 * @return Classified type.
	 */
	public int classify(Entity e1, Entity e2){
		int type = TYPE_UNCL;
		if ( this.isConjunction(e1, e2) )
			type = TYPE_CONJ;
		else if ( this.isParallel(e1, e2) )
			type = TYPE_PRLL;
		else if ( this.isRelayed(e1, e2) )
			type = TYPE_RLYD;
		else if ( this.isSerial(e1, e2) )
			type = TYPE_SRAL;
		return type;
	}
	
	/**
	 * Check if two entities are classified as relayed relationship.
	 * @param e1
	 * @param e2
	 * @return
	 */
	private boolean isRelayed(Entity e1, Entity e2){
		double ol = e1.overlap(e1);
		if ( ol <= 0 && Math.abs(ol) >= AEM.RLYD_TDIFF1 && Math.abs(ol) < AEM.RLYD_TDIFF2 )
			return true;
		return false;
	}
	
	/**
	 * Check if two entities are classified as serial relationship.
	 * @param e1
	 * @param e2
	 * @return
	 */
	private boolean isSerial(Entity e1, Entity e2){
		double ol = e1.overlap(e2);
		if ( ol <= 0 && Math.abs(ol) >= AEM.SRAL_TDIFF1 && Math.abs(ol) <= AEM.SRAL_TDIFF2)
			return true;
		return false;
	}
	
	/**
	 * Check if two entities are classified as conjunction relationship.
	 * @param e1
	 * @param e2
	 * @return
	 */
	private boolean isConjunction(Entity e1, Entity e2){
		double hd = e1.headDiff(e2);
		double td = e1.tailDiff(e2);
		double d1 = e1.duration();
		double d2 = e2.duration();
		// Get top private domains
		String sdm1 = getTopPrivateDomain(e1.url);
		String sdm2 = getTopPrivateDomain(e2.url);
		if ( hd <= AEM.CONJ_ST_DIFF && td/Math.min(d1, d2) < AEM.CONF_ET_PCRT &&
				sdm1 != null && sdm2 != null && sdm1.equals(sdm2))
			return true;
		return false;
	}
	
	private String getTopPrivateDomain(String url){
		String tpd = getHost(url);
		try {
			tpd = InternetDomainName.from(tpd).topPrivateDomain().toString();
		} catch (Exception e) {}
		return tpd; // maybe null
	}
	
	private String getHost(String url){
		if (url != null ){
			Pattern pattern = Pattern.compile("^(?:\\w+:?//)?([^:\\/\\?&]+)", Pattern.CASE_INSENSITIVE);
		    Matcher matcher = pattern.matcher(url);
		    if ( matcher.find() ){
		    	return matcher.group(1);
		    }
		}
	    return url;
	}
	
	/**
	 * Check if two entities are classified as parallel relationship.
	 * @param e1
	 * @param e2
	 * @return
	 */
	private boolean isParallel(Entity e1, Entity e2){
		double ol = e1.overlap(e2);
		if ( ol > AEM.PRLL_OL ) 
			return true;
		return false;
	}
}

/**
 * A POJO class representing an activity in AEM.
 * @author chenxm
 */
class Activity {
	// One activity, one tree.
	private TreeNode root = null; // 
	// A unique ID of this activity in the space of all activities.
	private UUID ID;
	
	/**
	 * Default constructor without actions.
	 */
	public Activity() {
		this.ID = UUID.randomUUID();
	}
	
	/**
	 * Initialize the activity with an entity and make this entity as root.
	 * @param entity
	 */
	public Activity(Entity entity){
		this();
		this.root=entity.getTreeNode();
	}
	
	/**
	 * Get the number of all entities in this activity.
	 * @return
	 */
	public int size(){
		return this.root.breadthFirstList().size();
	}
	
	/**
	 * Add an entity to this activity.
	 * The entity is identified by AEM being owned by the activity.
	 * @param entity
	 * @throws ExecException 
	 * @throws Exception 
	 */
	public void addEntity(Entity entity, Entity referrer) throws ExecException{
		if ( root == null && referrer != null )
			throw new ExecException("Parameter invalid: This activity is empty."); 
		if( root != null && referrer == null )
			throw new ExecException("An activity can not have two root entity.");
		if ( root == null && referrer == null )
			// add entity and treat it as the root
			this.root = entity.getTreeNode();
		if ( root != null && referrer != null ){
			// add entity and append it to its referred entity.
			if ( ! referrer.getTreeNode().isNodeAncestor(root))
				throw new ExecException("Given referrer entity does not exists in this activity.");
			referrer.getTreeNode().add(entity.getTreeNode());
		}
	}
	
	/**
	 * Check if this activity contains given entity.
	 * Return false if this activity is empty.
	 * @param entity
	 * @return
	 */
	public boolean contains(Entity entity){
		if ( root != null && entity.getTreeNode().isNodeAncestor(root) )
			return true;
		return false;
	}
	
	/**
	 * Remove given entity from this activity.
	 * At inner data structure, other entities linked to given entity are removed from this activity too.
	 * @param entity
	 * @return The removed entity (with linked other entities).
	 * @throws ExecException 
	 * @throws Exception
	 */
	public void removeEntity(Entity entity) throws ExecException{
		if ( root !=  null && ! root.equals(entity.getTreeNode())){
			Iterator<TreeNode> treeWalker = root.preorderIterator();
			while ( treeWalker.hasNext() ){
				TreeNode next = treeWalker.next();
				if ( next.isNodeChild(entity.getTreeNode())){
					next.remove(entity.getTreeNode());
					break;
				}
			}
		}
	}
	
	/**
	 * Find the referrer entity of given entity.
	 * @param entity
	 * @return The referrer entity if it is in the activity; otherwise, null is returned.
	 */
	public Entity findReferrerEntity(Entity entity){
		// All entities contained in this activity.
		Map<String,Entity> entities = new HashMap<String,Entity>(); // URL: entity
		if ( root == null )
			return null;
		Iterator<TreeNode> itr = root.preorderIterator();
		while( itr.hasNext() ){
			TreeNode node = itr.next();
			Entity e = (Entity) node.getObject();
			if ( e.url != null )
				entities.put(e.url, e);
		}
		// Find its referrer
		String ref = entity.referrer;
		if ( ref != null && entities.containsKey(ref)){
			return entities.get(ref);
		}
		return null;
	}
	
	public List<Entity> getAllEntities(){
		List<Entity> entities = new LinkedList<Entity>();
		if (root != null){
			Iterator<TreeNode> itr = root.preorderIterator();
			while( itr.hasNext() ){
				Entity e = (Entity)itr.next().getObject();
				if ( !e.isDummy )
					entities.add(e);
			}
		}
		return entities;
	}
	
	/**
	 * Check if this activity has root as given entity.
	 * @param entity
	 * @return
	 */
	public boolean hasRoot(Entity entity){
		return root.equals(entity.getTreeNode()) ? true : false;
	}
	
	/**
	 * Get the activity ID in a string format.
	 * @return ID string.
	 */
	public String getID(){
		return this.ID.toString();
	}
}

/**
 * A POJO class to represent entity in AEM.
 * @author chenxm
 */
class Entity {
	public Double start;	// the start time
	public Double end;	// the end time
	public String url;
	public String referrer;
	public String type;
	public Integer aemLastType = null; // classified type against the last entity.
	public Integer aemPredType = null; // classified type against its preceding entity.
	public boolean isDummy = false; // If this entity is dummy. 
	//E.g. if a referrer entity is lost but some entities refer to it, 
	//we create a dummy referrer entity to lead followers.
	public boolean hasFakeReferrer = false; // It indicates that this entity is linked to its preceding entity without referrer.
	
	private static final double DUR_LOW_BOUND = 0.1; //# sec, a lower-bound duration
	private String ID = null;
	private Tuple originalTuple = null;
	private TreeNode node = null; // the node this entity linked to.
	
	/**
	 * Initialize an entity with a tuple.
	 * @param tuple
	 * @throws ExecException
	 */
	public Entity(Tuple tuple) throws ExecException{
		this((Double)tuple.get(0),
			(Double)tuple.get(1),
			(String)tuple.get(2),
			(String)tuple.get(3),
			(String)tuple.get(4),
			(String)tuple.get(5),
			tuple);
	}
	
	/**
	 * Private initialization of an entity with six-fields.
	 * @param start
	 * @param end
	 * @param url
	 * @param referrer
	 * @param type
	 * @param id
	 */
	public Entity(Double start, Double end, String url, String referrer, String type, String id, Tuple original){
		this.start = start;
		if ( end == null )
			this.end = start + DUR_LOW_BOUND; // simply calculation to add a minimum duration.
		else
			this.end = end;
		this.url = url;
		this.referrer = referrer;
		this.type = type;
		this.ID = id;
		this.isDummy = false;
		this.hasFakeReferrer = false;
		this.node = new TreeNode(this);
		this.originalTuple = original;
		if ( original == null )
			this.isDummy = true;
	}
	
	public Tuple getTuple(){
		return this.originalTuple;
	}
	
	/**
	 * Get and set TreeNode of this entity.
	 */
	public TreeNode getTreeNode(){
		return this.node;
	}
	public void setTreeNode(TreeNode treeNode){
		this.node = treeNode;
	}
	
	/**
	 * Get the child number of this entity in underlying tree structure.
	 * @return
	 */
	public int getChildNum(){
		return this.node.getChildCount();
	}
	
	/**
	 * Get the overlap time duration of two entities;
	 * @param e
	 * @return overlapped duration: <0, =0, or >0
	 */
	public double overlap(Entity e){
		double s1 = this.start;
		double e1 = this.end;
		double s2 = e.start;
		double e2 = e.end;
		if (s2 >= s1)
			return Math.min(e1, e2) - s2;
		else
			return Math.min(e1, e2) - s1;
	}
	
	/**
	 * Get time difference of starting times of two entities.
	 * @param e
	 * @return Time difference
	 */
	public double headDiff(Entity e){
		double s1 = this.start;
		double s2 = e.start;
		return Math.abs(s1-s2);
	}
	
	/**
	 * Get time difference of ending times of two entities.
	 * @param e
	 * @return
	 */
	public double tailDiff(Entity e){
		double e1 = this.end;
		double e2 = e.end;
		return Math.abs(e1-e2);
	}
	
	/**
	 * Check if the entity is a base element of web page, e.g. a HTML file 
	 * which describes the textual content and framework of a web page.
	 * @param e
	 */
	public boolean isWebPageBase(){
		if ( this.type != null && this.type.contains("text"))
			return true;
		return false;
	}
	
	/**
	 * Get the duration of this entity.
	 * @return Elapsed time from start to end.
	 */
	public double duration(){
		return this.end - this.start;
	}
	
	/**
	 * Get unique ID of this entity.
	 * @return
	 */
	public String getID(){
		return this.ID.toString();
	}
	
	/**
	 * Check if this entity is the preceding of given e;
	 * @param e
	 * @return
	 */
	public boolean isPredEntity(Entity e){
		if ( this.url != null && e.referrer != null && this.url.equals(e.referrer))
			return true;
		return false;
	}
}