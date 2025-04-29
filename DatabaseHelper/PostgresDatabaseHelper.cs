using System;
using Microsoft.EntityFrameworkCore;
using project2_db_benchmark.Models.Postgres;
using project2_db_benchmark.Models.Shared;

namespace project2_db_benchmark.DatabaseHelper;

public class ApplicationDbContext : DbContext
{
    public DbSet<Business> Businesses { get; set; }
    public DbSet<Review> Reviews { get; set; }
    public DbSet<User> Users { get; set; }
    public DbSet<Checkin> Checkins { get; set; }
    public DbSet<Tip> Tips { get; set; }
    public DbSet<Photo> Photos { get; set; }
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseNpgsql($"Host=localhost;Port=5433;Username={Globals.POSTGRES_USER};Password={Globals.POSTGRES_USER};Database={Globals.POSTGRES_DB}");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Business configuration
        modelBuilder.Entity<Business>(entity =>
        {
            entity.HasKey(e => e.BusinessId);
            entity.HasIndex(e => e.Categories);
            entity.HasIndex(e => e.City);
            entity.HasIndex(e => e.Stars).IsDescending();

            entity.Property(e => e.Attributes).HasColumnType("jsonb");
            entity.Property(e => e.Hours).HasColumnType("jsonb");
        });

        // Review configuration
        modelBuilder.Entity<Review>(entity =>
        {
            entity.HasKey(e => e.ReviewId);
            entity.HasIndex(e => e.BusinessId);
            entity.HasIndex(e => e.UserId);
            entity.HasIndex(e => e.Date).IsDescending();
        });

        // User configuration
        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(e => e.UserId);
            entity.HasIndex(e => e.Name);

            entity.Property(e => e.Friends).HasColumnType("text[]");
            entity.Property(e => e.Elite).HasColumnType("integer[]");
        });

        // Checkin configuration
        modelBuilder.Entity<Checkin>(entity =>
        {
            entity.HasKey(e => new { e.BusinessId, e.Date });
            entity.HasIndex(e => e.BusinessId);
        });

        // Tip configuration
        modelBuilder.Entity<Tip>(entity =>
        {
            entity.HasKey(e => new { e.BusinessId, e.UserId, e.Date });
            entity.HasIndex(e => e.BusinessId);
            entity.HasIndex(e => e.UserId);
            entity.HasIndex(e => e.Date).IsDescending();
        });

        // Photo configuration
        modelBuilder.Entity<Photo>(entity =>
        {
            entity.HasKey(e => e.PhotoId);
            entity.HasIndex(e => e.BusinessId);
        });
    }
}

public class PostgresDatabaseHelper
{
    private readonly ApplicationDbContext _context;

    public PostgresDatabaseHelper()
    {
        _context = new ApplicationDbContext();
        _context.Database.EnsureCreated(); // This creates the database and tables if they don't exist
    }

    public async Task InsertBusinessAsync(Business business)
    {
        await _context.Businesses.AddAsync(business);
        await _context.SaveChangesAsync();
    }

    public async Task InsertReviewAsync(Review review)
    {
        await _context.Reviews.AddAsync(review);
        await _context.SaveChangesAsync();
    }

    public async Task InsertUserAsync(User user)
    {
        await _context.Users.AddAsync(user);
        await _context.SaveChangesAsync();
    }

    public async Task InsertCheckinAsync(Checkin checkin)
    {
        await _context.Checkins.AddAsync(checkin);
        await _context.SaveChangesAsync();
    }

    public async Task InsertTipAsync(Tip tip)
    {
        await _context.Tips.AddAsync(tip);
        await _context.SaveChangesAsync();
    }

    public async Task InsertPhotoAsync(Photo photo)
    {
        await _context.Photos.AddAsync(photo);
        await _context.SaveChangesAsync();
    }
}
