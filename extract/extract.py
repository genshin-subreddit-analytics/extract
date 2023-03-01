def main():
    # Send email: extraction starting
    try:  # Acquiring, Cleaning, and Storing Data
        # Get Status (from Database? S3?)
        # Get Data
        # Build DataFrame
        # Clean DataFrame
        # Write DataFrame to S3
        pass
    except:  # Handle Failure
        # Send email: extraction failed, why failed
        pass
    else:  # Handle Success
        # Send email: extraction succesful, num of data parsed
        pass


if __name__ == "__main__":
    main()
