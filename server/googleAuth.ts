import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import session from "express-session";
import type { Express, RequestHandler } from "express";
import connectPg from "connect-pg-simple";
import { storage } from "./storage";

export function getSession() {
  const sessionTtl = 30 * 24 * 60 * 60 * 1000; // 30 days
  const pgStore = connectPg(session);
  const sessionStore = new pgStore({
    conString: process.env.DATABASE_URL,
    createTableIfMissing: false,
    ttl: sessionTtl,
    tableName: "sessions",
  });
  return session({
    secret: process.env.SESSION_SECRET!,
    store: sessionStore,
    resave: false,
    saveUninitialized: false,
    rolling: true,
    cookie: {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      maxAge: sessionTtl,
      sameSite: "lax",
    },
  });
}

async function upsertUserFromGoogle(profile: any): Promise<string> {
  const email = profile.emails?.[0]?.value;
  const googleId = profile.id;
  const firstName = profile.name?.givenName || "";
  const lastName = profile.name?.familyName || "";
  const profileImageUrl = profile.photos?.[0]?.value || "";

  // Look up existing user by email to preserve their existing ID and data
  if (email) {
    const existingByEmail = await storage.getUserByEmail(email);
    if (existingByEmail) {
      // Update their info but keep their existing ID and workspace
      await storage.upsertUser({
        id: existingByEmail.id,
        email,
        firstName,
        lastName,
        profileImageUrl,
        workspaceId: existingByEmail.workspaceId,
        role: existingByEmail.role,
        isActive: existingByEmail.isActive,
      });
      return existingByEmail.id;
    }
  }

  // New user — check if this is the owner
  const isOwner = email === "egblinds@gmail.com";
  const userId = `google:${googleId}`;

  if (isOwner) {
    await storage.upsertUser({
      id: userId,
      email: email || "",
      firstName,
      lastName,
      profileImageUrl,
      workspaceId: 1,
      role: "owner",
      isActive: true,
    });
  } else {
    const existingById = await storage.getUser(userId);
    if (!existingById) {
      const workspace = await storage.createWorkspace({
        name: `${firstName || email || "User"}'s Workspace`,
        ownerId: userId,
      });
      await storage.upsertUser({
        id: userId,
        email: email || "",
        firstName,
        lastName,
        profileImageUrl,
        workspaceId: workspace.id,
        role: "owner",
        isActive: true,
      });
    }
  }

  return userId;
}

export async function setupAuth(app: Express) {
  app.set("trust proxy", 1);
  app.use(getSession());
  app.use(passport.initialize());
  app.use(passport.session());

  const callbackURL = process.env.GOOGLE_CALLBACK_URL || "http://localhost:5000/api/callback";

  const clientID = process.env.GOOGLE_CLIENT_ID!;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;

  passport.use(
    new GoogleStrategy(
      {
        clientID,
        clientSecret,
        callbackURL,
        scope: ["profile", "email"],
      },
      async (_accessToken, _refreshToken, profile, done) => {
        try {
          const userId = await upsertUserFromGoogle(profile);
          // Maintain the same req.user.claims.sub format as before
          const user = {
            claims: {
              sub: userId,
              email: profile.emails?.[0]?.value,
              first_name: profile.name?.givenName,
              last_name: profile.name?.familyName,
            },
          };
          done(null, user);
        } catch (err) {
          done(err as Error);
        }
      }
    )
  );

  passport.serializeUser((user: any, cb) => cb(null, user));
  passport.deserializeUser((user: any, cb) => cb(null, user));

  app.get(
    "/api/login",
    passport.authenticate("google", {
      scope: ["profile", "email"],
      prompt: "select_account",
    })
  );

  app.get(
    "/api/callback",
    passport.authenticate("google", {
      successReturnToOrRedirect: "/",
      failureRedirect: "/api/login",
    })
  );

  app.get("/api/logout", (req, res) => {
    req.logout(() => {
      res.redirect("/");
    });
  });
}

export const isAuthenticated: RequestHandler = async (req, res, next) => {
  if (!req.isAuthenticated()) {
    return res.status(401).json({ message: "Unauthorized" });
  }
  return next();
};

export const isOwner: RequestHandler = async (req, res, next) => {
  const user = req.user as any;
  const userId = user.claims.sub;
  const dbUser = await storage.getUser(userId);

  if (!dbUser || dbUser.role !== "owner") {
    return res.status(403).json({ message: "Owner access required" });
  }

  next();
};

export const isActive: RequestHandler = async (req, res, next) => {
  const user = req.user as any;
  const userId = user.claims.sub;
  const dbUser = await storage.getUser(userId);

  if (!dbUser || !dbUser.isActive) {
    return res.status(403).json({ message: "Account not approved" });
  }

  next();
};

export const requireRole = (requiredRoles: string[]): RequestHandler => {
  return async (req, res, next) => {
    const user = req.user as any;
    const userId = user.claims.sub;
    const dbUser = await storage.getUser(userId);

    if (!dbUser || !requiredRoles.includes(dbUser.role)) {
      return res.status(403).json({
        message: `Access denied. Required role: ${requiredRoles.join(" or ")}`,
      });
    }

    next();
  };
};

export const requireWriteAccess: RequestHandler = async (req, res, next) => {
  const user = req.user as any;
  const userId = user.claims.sub;
  const dbUser = await storage.getUser(userId);

  if (!dbUser || dbUser.role === "viewer") {
    return res.status(403).json({
      message: "Read-only access. Contact workspace owner for write permissions.",
    });
  }

  next();
};
