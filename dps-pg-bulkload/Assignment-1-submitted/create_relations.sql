ALTER TABLE authors ADD CONSTRAINT authors_pk PRIMARY KEY (id);
ALTER TABLE subreddits ADD CONSTRAINT subreddits_pk PRIMARY KEY (id);
ALTER TABLE submissions ADD CONSTRAINT submissions_pk PRIMARY KEY (id);
ALTER TABLE comments ADD CONSTRAINT comments_pk PRIMARY KEY (id);

ALTER TABLE authors ADD CONSTRAINT authors_name_uk UNIQUE (name);
ALTER TABLE subreddits ADD CONSTRAINT subreddits_name_uk UNIQUE (name);
ALTER TABLE subreddits ADD CONSTRAINT subreddits_display_name_uk UNIQUE (display_name);

ALTER TABLE submissions
  ADD CONSTRAINT submissions_author_fk
  FOREIGN KEY (author)
  REFERENCES authors(name)
  ON DELETE CASCADE;

ALTER TABLE submissions
  ADD CONSTRAINT submissions_subreddit_fk
  FOREIGN KEY (subreddit_id)
  REFERENCES subreddits(name)
  ON DELETE CASCADE;

ALTER TABLE comments
  ADD CONSTRAINT comments_author_fk
  FOREIGN KEY (author)
  REFERENCES authors(name)
  ON DELETE CASCADE;

ALTER TABLE comments
  ADD CONSTRAINT comments_subreddit_fk
  FOREIGN KEY (subreddit_id)
  REFERENCES subreddits(name)
  ON DELETE CASCADE;

ALTER TABLE comments
  ADD CONSTRAINT comments_subreddit_id_fk
  FOREIGN KEY (subreddit)
  REFERENCES subreddits(display_name)
  ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(author);
CREATE INDEX IF NOT EXISTS idx_comments_subreddit_id ON comments(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_comments_subreddit ON comments(subreddit);
CREATE INDEX IF NOT EXISTS idx_submissions_author ON submissions(author);
CREATE INDEX IF NOT EXISTS idx_submissions_subreddit ON submissions(subreddit_id);
