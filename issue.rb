require 'securerandom'
require "bundler/inline"

gemfile(true) do
  source "https://rubygems.org"

  git_source(:github) { |repo| "https://github.com/#{repo}.git" }

  gem "rails", github: "rails/rails"
  gem "pg"
end

require "active_record"
require "minitest/autorun"
require "logger"

conn_config = {
  dbname: 'postgres'
}
db_name = "verify_issue_#{SecureRandom.hex}"
db_conn = PG.connect(conn_config)
db_conn.exec("CREATE DATABASE #{db_name}")


# This connection will do for database-independent bug reports.
ActiveRecord::Base.establish_connection(adapter: "postgresql", database: db_name)
ActiveRecord::Base.logger = Logger.new(STDOUT)

ActiveRecord::Schema.define do
  create_table :users, force: :cascade do |t|
    t.string :name, null: false
  end

  create_table :posts, force: true do |t|
    t.string :title, null: false
  end

  create_table :activations, force: :cascade do |t|
    t.bigint :user_id, null: false
    t.bigint :post_id, null: false
    t.date :activated_at, null: false
  end
  add_foreign_key "activations", "users", on_delete: :cascade
  add_foreign_key "activations", "posts", on_delete: :cascade
end

class User < ActiveRecord::Base
  has_many :activations
  has_many :posts, through: :activations
end

class Post < ActiveRecord::Base
  has_many :activations
  has_many :users, through: :activations

  # for #4
  has_many :latest_activations, -> { where(id: latests) }, class_name: 'Activation'
end

class Activation < ActiveRecord::Base
  belongs_to :user, dependent: :delete
  belongs_to :post, dependent: :delete

  # for #4
  scope :latests, -> { select('DISTINCT ON (user_id) id').order('user_id, activated_at DESC') }
end

require 'minitest/autorun'
describe 'verify "DISTINCT ON" queries' do
  before :all do
    user = User.create(name: 'user')
    @old_post = Post.create(title: 'old_post')
    @new_post = Post.create(title: 'new_post')
    Activation.create(user: user, post: @old_post, activated_at: '2020-01-01')
    Activation.create(user: user, post: @new_post, activated_at: '2020-01-02')
  end

  # NG!
  it "#1 EXPECTED" do
    latest_activated_posts_each_users = Post.joins(:activations).select("DISTINCT ON (user_id) *").order("user_id, activated_at DESC")
    assert_equal latest_activated_posts_each_users, [@new_post]
    assert_equal latest_activated_posts_each_users.first, @new_post
    assert_equal latest_activated_posts_each_users.size, 1
  end

  it '#1 NOT EXPECTED' do
    latest_activated_posts_each_users = Post.joins(:activations).select("DISTINCT ON (user_id) *").order("user_id, activated_at DESC")
    assert_equal latest_activated_posts_each_users.to_sql, %(
    SELECT DISTINCT ON (user_id) *
    FROM "posts"
    INNER JOIN "activations" ON "activations"."post_id" = "posts"."id"
    ORDER BY user_id,
             activated_at DESC
    ).gsub(/\s{1,}/, "\s").strip
    assert_equal latest_activated_posts_each_users.where(title: 'old_post'), [@old_post]
    assert_raises ActiveRecord::StatementInvalid do
      latest_activated_posts_each_users.count
    end
  end

  # OK!
  it '#2 EXPECTED: Using sub-query into "AR.from" method' do
    latest_activated_posts_each_users = Post.from(Post.joins(:activations).select("DISTINCT ON (user_id) *").order("user_id, activated_at DESC"), :posts)
    assert_equal latest_activated_posts_each_users.to_sql, %(
    SELECT "posts".*
    FROM
      (SELECT DISTINCT ON (user_id) *
       FROM "posts"
       INNER JOIN "activations" ON "activations"."post_id" = "posts"."id"
       ORDER BY user_id,
                activated_at DESC) posts
    ).gsub(/\s{1,}/, "\s").strip
    assert_equal latest_activated_posts_each_users, [@new_post]
    assert_equal latest_activated_posts_each_users.first, @new_post
    assert_equal latest_activated_posts_each_users.where(title: 'old_post'), []
    assert_equal latest_activated_posts_each_users.size, 1
    assert_equal latest_activated_posts_each_users.count, 1
  end

  it '#3 EXPECTED: Using sub-query into "AR.where" method' do
    latest_activated_posts_each_users = Post.joins(:activations).where(activations: { id: Activation.select('DISTINCT ON (user_id) id').order('user_id, activated_at DESC') })
    assert_equal latest_activated_posts_each_users.to_sql, %(
    SELECT "posts".*
    FROM "posts"
    INNER JOIN "activations" ON "activations"."post_id" = "posts"."id"
    WHERE "activations"."id" IN
          (SELECT DISTINCT ON (user_id) id
           FROM "activations"
           ORDER BY user_id,
                    activated_at DESC)
    ).gsub(/\s{1,}/, "\s").strip
    assert_equal latest_activated_posts_each_users, [@new_post]
    assert_equal latest_activated_posts_each_users.first, @new_post
    assert_equal latest_activated_posts_each_users.where(title: 'old_post'), []
    assert_equal latest_activated_posts_each_users.size, 1
    assert_equal latest_activated_posts_each_users.count, 1
  end

  it '#4 EXPECTED: Using AR-scope' do
    latest_activated_posts_each_users = Post.joins(:latest_activations)
    assert_equal latest_activated_posts_each_users.to_sql, %(
    SELECT "posts".*
    FROM "posts"
    INNER JOIN "activations" ON "activations"."post_id" = "posts"."id"
    AND "activations"."id" IN
      (SELECT DISTINCT ON (user_id) id
       FROM "activations"
       ORDER BY user_id,
                activated_at DESC)
    ).gsub(/\s{1,}/, "\s").strip
    assert_equal latest_activated_posts_each_users, [@new_post]
    assert_equal latest_activated_posts_each_users.first, @new_post
    assert_equal latest_activated_posts_each_users.where(title: 'old_post'), []
    assert_equal latest_activated_posts_each_users.size, 1
    assert_equal latest_activated_posts_each_users.count, 1
  end

  after :all do
    Activation.delete_all
  end
end
Minitest.after_run do
  ActiveRecord::Base.remove_connection
  db_conn.exec("DROP DATABASE #{db_name}")
end


