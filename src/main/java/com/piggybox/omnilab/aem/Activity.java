package com.piggybox.omnilab.aem;

import com.piggybox.utils.tree.TreeNode;
import org.apache.pig.backend.executionengine.ExecException;

import java.util.*;

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
     * @throws org.apache.pig.backend.executionengine.ExecException
     * @throws Exception
     */
    public void addEntity(Entity entity, Entity referrer) throws ExecException {
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