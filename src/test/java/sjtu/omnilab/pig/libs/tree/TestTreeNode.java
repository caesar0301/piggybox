package sjtu.omnilab.pig.libs.tree;


import sjtu.omnilab.pig.utils.tree.TreeNode;
import sjtu.omnilab.pig.utils.tree.TreeString;

public class TestTreeNode {
	public static void main(String[] args){
		TreeNode node1 = new TreeNode("1");
		TreeNode node2 = new TreeNode("2");
		TreeNode node3 = new TreeNode("3");
		TreeNode node4 = new TreeNode("4");
		
		node1.add(node2);
		node1.add(node3);
		node2.add(node4);
		
		node1.remove(node2);
		
		for ( TreeNode node : node1.children())
			System.out.println(node);
		System.out.println(node1.children().size());
		
		System.out.println(node1.dominates(node4));
		
		System.out.println(TreeString.toString(node1));
		System.out.println(TreeString.toString(node2));
		
		System.out.println(node2.isNodeChild(node2));
	}
}
