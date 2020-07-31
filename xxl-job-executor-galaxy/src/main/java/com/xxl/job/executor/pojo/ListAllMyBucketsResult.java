package com.xxl.job.executor.pojo;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "ListAllMyBucketsResult")
public class ListAllMyBucketsResult {

	private Owner owner;

	private Buckets buckets;

	
	public static class Owner {

		private String id;

		private String displayName;
		@XmlElement(name="ID")
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
		@XmlElement(name="DisplayName")
		public String getDisplayName() {
			return displayName;
		}

		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}

	}

	public static class Buckets {

		private List<Bucket> bucket;

		public static class Bucket {
			
			private String name;

			private String creationDate;
			@XmlElement(name="Name")
			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}
			@XmlElement(name="CreationDate")
			public String getCreationDate() {
				return creationDate;
			}

			public void setCreationDate(String creationDate) {
				this.creationDate = creationDate;
			}

			@Override
			public String toString() {
				return "Bucket [name=" + name + ", creationDate=" + creationDate + "]";
			}
			
		}
		@XmlElement(name="Bucket")
		public List<Bucket> getBucket() {
			return bucket;
		}

		public void setBucket(List<Bucket> bucket) {
			this.bucket = bucket;
		}

		@Override
		public String toString() {
			return "Buckets [bucket=" + bucket + "]";
		}

	}

	@XmlElement(name="Owner")
	public Owner getOwner() {
		return owner;
	}

	public void setOwner(Owner owner) {
		this.owner = owner;
	}
	@XmlElement(name="Buckets")
	public Buckets getBuckets() {
		return buckets;
	}

	public void setBuckets(Buckets buckets) {
		this.buckets = buckets;
	}

	@Override
	public String toString() {
		return "ListAllMyBucketsResult [owner=" + owner + ", buckets=" + buckets + "]";
	}
	
}
